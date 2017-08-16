#!/usr/bin/env python
# Author: catlittlechen@gmail.com
# encoding: utf-8


ClusterHashSlots = 16384
MigrateDefaultTimeout = 60000
MigrateDefaultPipeline = 10
RebalanceDefaultThreshold = 2

import random
import time
from math import ceil, floor
from redis_node import ClusterNode, createRedisNode


class RedisControl():

    def __init__(self, cluster_addr=None, replicas=1, fix=False, pipeline=None):
        self.nodes = set()
        self.fix = fix
        self.errors = []
        self.timeout = MigrateDefaultTimeout
        self.replicas = replicas
        self.cluster_addr = cluster_addr
        self.pipeline = pipeline
        if not self.pipeline:
            self.pipeline = MigrateDefaultPipeline
        if self.cluster_addr:
            self.load_cluster_info_from_node(cluster_addr)
            self.check_cluster()
            if len(self.errors) != 0:
                print("*** Please fix your cluster problems")
                exit(1)
        return

    def add_node(self, node):
        for n in self.nodes:
            if n.info["addr"] == node.info["addr"]:
                print("ERR node exists")
        self.nodes.add(node)
        return

    def reset_nodes(self):
        for node in self.nodes:
            node.r.close()
        self.nodes = set()
        return

    def reload_info(self):
        self.wait_cluster_join()
        self.reset_nodes()
        self.load_cluster_info_from_node(self.cluster_addr)
        self.check_cluster()
        time.sleep(1)

    def cluster_error(self, msg):
        self.errors.append(msg)
        print(msg)
        return

    # Return the node with the specified ID or Nil.
    def get_node_by_name(self, name):
        for n in self.nodes:
            if n.full_name() == name.lower():
                return n
        return None

    # This function returns the master that has the least number of replicas
    # in the cluster. If there are multiple masters with the same smaller
    # number of replicas, one at random is returned.
    def get_master_with_least_replicas(self, other=None):
        master_nodes = []
        for n in self.nodes:
            if n.has_flag("master"):
                if other and other == n:
                    continue
                master_nodes.append(n)
        if len(master_nodes) == 0:
            return None
        master_nodes = sorted(master_nodes, key=lambda x: x.replicas_length())
        return master_nodes[0]

    def check_cluster(self, quiet=False):
        print(">>> Performing Cluster Check (using node %s)" % list(self.nodes)[0])
        if quiet:
            self.show_nodes()
        self.check_config_consistency()
        self.check_open_slots()
        self.check_slots_coverage()

    def show_clsuter_info(self):
        masters = 0
        keys = 0
        for n in self.nodes:
            if n.has_flag("master"):
                print("%s (%s) -> %s keys | %d slots | " % (n, n.full_name(), n.dbsize(), n.slots_length()))
                masters += 1
                keys += n.dbsize()
        print("[OK] %d keys in %d masters." % (keys, masters))
        print("%.2f keys per slot on average." % 1.0 * keys / ClusterHashSlots)
        return

    # Merge slots of every known node. If the resulting slots are equal
    # to ClusterHashSlots, then all slots are served.
    def covered_slots(self):
        slots = set()
        for n in self.nodes:
            slots = slots.union(n.get_slots())
        return slots

    def check_slots_coverage(self):
        print(">>> Check slots coverage...")
        slots = self.covered_slots()
        print("covered_slots: %d" % len(slots))
        if len(slots) == ClusterHashSlots:
            print("[OK] All %d slots covered." % ClusterHashSlots)
        else:
            self.cluster_error("[ERR] Not all %d slots are covered by nodes." % ClusterHashSlots)
            if self.fix:
                self.fix_slots_coverage()

    def check_open_slots(self):
        print(">>> Check for open slots...")
        open_slots = set()
        for n in self.nodes:
            if len(n.info["migrating"]) > 0:
                keys = n.info["migrating"].keys()
                self.cluster_error("[WARNING] Node %s has slots in migrating state (%s)."% (n, ",".join("%s" % k for k in keys)))
                open_slots = open_slots.union(set(keys))
            if len(n.info["importing"]) > 0:
                keys = n.info["importing"].keys()
                self.cluster_error("[WARNING] Node %s has slots in importing state (%s)."% (n, ",".join("%s" % k for k in keys)))
                open_slots = open_slots.union(set(keys))
        if len(open_slots) > 0:
            print("[WARNING] The following slots are open: %s" % ",".join("%s" % n for n in open_slots))
        if self.fix:
            for s in open_slots:
                self.fix_open_slot(s)
        return

    def nodes_with_keys_in_slot(self, slot):
        nodes= []
        for n in self.nodes:
            if n.has_flag("master") and n.has_keys(slot):
                nodes.append(n)
        return nodes

    def fix_slots_coverage(self):
        covered_slots = self.covered_slots()
        not_covered = []
        for s in range(ClusterHashSlots):
            if s not in covered_slots:
                not_covered.append(s)
        print(">>> Fixing slots coverage...")

        # For every slot, take action depending on the actual condition:
        # 1) No node has keys for this slot.
        # 2) A single node has keys for this slot.
        # 3) Multiple nodes have keys for this slot.
        slots = {}
        for slot in not_covered:
            nodes = self.nodes_with_keys_in_slot(slot)
            slots[slot] = nodes
            print("Slot %s has keys in %d nodes: %s" % (slot, len(nodes), ",".join("%s" % n for n in nodes)))

        none = {}
        single = {}
        multi = {}
        for slot in slots:
            length = len(slots[slot])
            if length == 0:
                none[slot] = slots[slot]
            elif length == 1:
                single[slot] = slots[slot]
            else:
                multi[slot] = slots[slot]

        # Handle case "1": keys in no node.
        if len(none) > 0:
            print("The folowing uncovered slots have no keys across the cluster:")
            for slot in none:
                node = list(self.nodes)[random.randint(0, len(self.nodes))-1]
                print(">>> Covering slot %s with %s" % (slot, node))
                node.r.execute("cluster", "addslots", slot)

        # Handle case "2": keys only in one node.
        if len(single) > 0:
            print("The folowing uncovered slots have keys in just one node:")
            for slot in single:
                node = single[slot][0]
                print(">>> Covering slot %s with %s" % (slot, node))
                node.r.execute("cluster", "addslots", slot)

        # Handle case "3": keys in multiple nodes.
        if len(multi) > 0:
            print("The folowing uncovered slots have keys in multiple nodes:")
            print(",".join(multi.keys()))
            for slot in multi:
                target, left = self.get_node_with_most_keys_in_slot(multi[slot], slot)
                print(">>> Covering slot %s moving keys to %s" % (slot, target))

                target.r.execute("cluster", "addslots", slot)
                target.r.execute("cluster", "setslot", slot, "stable")

                for src in left:
                    # Set the source node in 'importing' state (even if we will
                    # actually migrate keys away) in order to avoid receiving
                    # redirections for MIGRATE.
                    src.r.execute("cluster", "setslot", slot, "importing", target.full_name())
                    self.move_slot(src, target, slot, {"fix":True, "cold":True})
                    src.r.execute("cluster", "setslot", slot, "stable")

    # Return the owner of the specified slot
    def get_slot_owners(self, slot):
        owners = []
        for n in self.nodes:
            if n.has_flag("slave"):
                continue
            if n.has_slot(slot):
                owners.append(n)
        return owners

    # Return the node, among 'nodes' with the greatest number of keys
    # in the specified slot.
    def get_node_with_most_keys_in_slot(self, nodes, slot):
        best = None
        best_numkeys = 0
        left = []
        for n in self.nodes:
            if n.has_flag("slave"):
                continue
            numkeys = n.r.execute("cluster", "countkeysinslot", slot)
            if numkeys > best_numkeys or best is None:
                if best:
                    left.append(best)
                best = n
                best_numkeys = numkeys
        return best, left

    # Slot 'slot' was found to be in importing or migrating state in one or
    # more nodes. This function fixes this condition by migrating keys where
    # it seems more sensible.
    def fix_open_slot(self, slot):
        print(">>> Fixing open slot %s" % slot)
        # Try to obtain the current slot owner, according to the current
        # nodes configuration.
        owners = self.get_slot_owners(slot)
        owner = None
        if len(owners) == 1:
            owner = owners[0]

        migrating = set()
        importing = set()
        for n in self.nodes:
            if n.has_flag("slave"):
                continue
            if slot in n.info["migrating"]:
                migrating.add(n)
            elif slot in n.info["importing"]:
                importing.add(n)
            elif n.r.execute("cluster", "countkeysinslot", slot) > 0 and n != owner:
                print("*** Found keys about slot %s in node %s!"% (slot, n))
                importing.add(n)

        print("Set as migrating in: %s" % ",".join("%s" % n for n in migrating))
        print("Set as importing in: %s" % ",".join("%s" % n for n in importing))

        # If there is no slot owner, set as owner the slot with the biggest
        # number of keys, among the set of migrating / importing nodes.
        if owner is None:
            print(">>> Nobody claims ownership, selecting an owner...")
            owner = self.get_node_with_most_keys_in_slot(self.nodes, slot)

            # If we still don't have an owner, we can't fix it.
            if owner is None:
                print("[ERR] Can't select a slot owner. Impossible to fix.")
                exit(1)

            # Use ADDSLOTS to assign the slot.
            print("*** Configuring %s as the slot owner" % owner)
            owner.r.execute("cluster", "setslot", slot, "stable")
            owner.r.execute("cluster", "addslots", slot)
            # Make sure this information will propagate. Not strictly needed
            # since there is no past owner, so all the other nodes will accept
            # whatever epoch this node will claim the slot with.
            owner.r.execute("cluster", "bumpepoch")

            # Remove the owner from the list of migrating/importing
            # nodes.
            migrating.remove(owner)
            importing.remove(owner)

        migrating = list(migrating)
        importing = list(importing)

        # If there are multiple owners of the slot, we need to fix it
        # so that a single node is the owner and all the other nodes
        # are in importing state. Later the fix can be handled by one
        # of the base cases above.
        #
        # Note that this case also covers multiple nodes having the slot
        # in migrating state, since migrating is a valid state only for
        # slot owners.
        if len(owners) > 1:
            owner = self.get_node_with_most_keys_in_slot(owners, slot)
            for n in owners:
                if n.full_name() == owner.full_name():
                    continue
                n.r.execute("cluster", "delslots", slot)
                n.r.execute("cluster", "setslot", slot, "importing", owner.full_name())
                importing.add(n.full_name())
            owner.r.execute("cluster", "bumpepoch")

        # Case 1: The slot is in migrating state in one slot, and in
        #         importing state in 1 slot. That's trivial to address.
        if len(migrating) == 1 and len(importing) == 1:
            self.move_slot(migrating[0], importing[0], slot, {"fix":True})
        # Case 2: There are multiple nodes that claim the slot as importing,
        # they probably got keys about the slot after a restart so opened
        # the slot. In this case we just move all the keys to the owner
        # according to the configuration.
        elif len(migrating) == 0 and len(importing) > 0:
            print(">>> Moving all the %s slot keys to its owner %s" % (slot, owner))
            for node in importing:
                if node == owner:
                    continue
                self.move_slot(node, owner, slot, {"fix":True, "cold":True})
                print(">>> Setting %s as STABLE in %s" % (slot, node))
                node.r.execute("cluster", "setslot", slot, "stable")
        # Case 3: There are no slots claiming to be in importing state, but
        # there is a migrating node that actually don't have any key. We
        # can just close the slot, probably a reshard interrupted in the middle.
        elif len(importing) == 0 and len(migrating) == 1 and len(migrating[0].r.execute("cluster", "getkeysinslot", slot, 1)) == 0:
            migrating[0].r.execute("cluster", "setslot", slot, "stable")
        else:
            print("[ERR] Sorry, can't fix this slot yet (work in progress). Slot is set as migrating in %s, as importing in %s, owner is %s" % (",".join("%s" % n for n in migrating), ",".join("%s" % n for n in importing), owner))

    # Check if all the nodes agree about the cluster configuration
    def check_config_consistency(self):
        if self.is_config_consistent():
            print("[OK] All nodes agree about slots configuration.")
        else:
            self.cluster_error("[ERR] Nodes don't agree about configuration!")
        return

    def is_config_consistent(self):
        sign = None
        for n in self.nodes:
            if not sign:
                sign = n.get_config_signature()
                continue
            if sign != n.get_config_signature():
                return False
        return True

    def wait_cluster_join(self):
        print("Waiting for the cluster to join")
        while not self.is_config_consistent():
            print(".")
            time.sleep(1)
        return

    def alloc_slots(self):
        nodes_count = len(self.nodes)
        masters_count = int(len(self.nodes) / (self.replicas+1))
        masters = []

        # The first step is to split instances by IP. This is useful as
        # we'll try to allocate master nodes in different physical machines
        # (as much as possible) and to allocate slaves of a given master in
        # different physical machines as well.
        #
        # This code assumes just that if the IP is different, than it is more
        # likely that the instance is running in a different physical host
        # or at least a different virtual machine.
        ips = {}
        for n in sorted(list(self.nodes), key=lambda x: x.info["addr"]):
            ips.setdefault(n.info["host"], [])
            ips[n.info["host"]].append(n)

        # Select master instances
        print("Using %d masters:" % masters_count)
        interleaved = []

        while len(ips) != 0:
            # Take one node from each IP until we run out of nodes
            # across every IP.
            for ip in ips:
                nodes = ips[ip]
                if len(nodes) == 0:
                    del ips[ip]
                    break
                else:
                    # else, move one node from this IP to 'interleaved'
                    interleaved.append(nodes[0])
                    del nodes[0]

        masters = interleaved[:masters_count]
        interleaved = set(interleaved[masters_count:])
        nodes_count -= len(masters)

        # Alloc slots on masters
        slots_per_node = float(ClusterHashSlots) / masters_count
        first = 0
        cursor = 0.0
        masternum = 0
        for n in masters:
            last = int(cursor+slots_per_node-1)
            if last > ClusterHashSlots or masternum == len(masters)-1:
                last = ClusterHashSlots-1
            if last < first:
                last = first # Min step is 1.
            n.add_slots(first, last)
            first = last+1
            cursor += slots_per_node
            masternum += 1

        # Select N replicas for every master.
        # We try to split the replicas among all the IPs with spare nodes
        # trying to avoid the host where the master is running, if possible.
        #
        # Note we loop two times.  The first loop assigns the requested
        # number of replicas to each master.  The second loop assigns any
        # remaining instances as extra replicas to masters.  Some masters
        # may end up with more than their requested number of replicas, but
        # all nodes will be used.
        assignment_verbose = True
        requested = "requested"
        unused = "unused"

        for assign in [requested, unused]:
            for m in masters:
                assigned_replicas = 0
                while assigned_replicas < self.replicas:
                    if nodes_count == 0:
                        break
                    if assignment_verbose:
                        if assign == requested:
                            print("Requesting total of %s replicas (%d replicas assigned so far with %d total remaining)." % (self.replicas, assigned_replicas, nodes_count))
                        elif assign == unused:
                            print("Assigning extra instance to replication role too (%d remaining)." % nodes_count)

                    # Return the first node not matching our current master
                    node = None
                    for n in interleaved:
                        if n.info["host"] != m.info["host"]:
                            node = n
                            break

                    # If we found a node, use it as a best-first match.
                    # Otherwise, we didn't find a node on a different IP, so we
                    # go ahead and use a same-IP replica.
                    if node:
                        slave = node
                        interleaved.remove(node)
                    else:
                        slave = interleaved.pop()
                    print("slave %s set %s as replicate" % (slave, m))
                    slave.set_as_replicate(m.full_name())
                    nodes_count -= 1
                    assigned_replicas += 1
                    print("Adding replica %s to %s" % (slave, m))

                    # If we are in the "assign extra nodes" loop,
                    # we want to assign one extra replica to each
                    # master before repeating masters.
                    # This break lets us assign extra replicas to masters
                    # in a round-robin way.
                    if assign == unused:
                        break
        return

    def flush_nodes_config(self):
        for n in self.nodes:
            n.flush_node_config()

    # Redis Cluster config epoch collision resolution code is able to eventually
    # set a different epoch to each node after a new cluster is created, but
    # it is slow compared to assign a progressive config epoch to each node
    # before joining the cluster. However we do just a best-effort try here
    # since if we fail is not a problem.
    def assign_config_epoch(self):
        config_epoch = 1
        for n in self.nodes:
            n.r.execute("cluster", "set-config-epoch", config_epoch)
            config_epoch += 1

    def join_cluster(self):
        # We use a brute force approach to make sure the node will meet
        # each other, that is, sending CLUSTER MEET messages to all the nodes
        # about the very same node.
        # Thanks to gossip this information should propagate across all the
        # cluster in a matter of seconds.
        first = False
        for n in self.nodes:
            if first is False:
                first = n.info
                continue
            print("cluster meet %s to %s:%d" % (n, first["host"], first["port"]))
            n.r.execute("cluster", "meet", first["host"], first["port"])

    def load_cluster_info_from_node(self, nodeaddr):
        node = createRedisNode(nodeaddr)
        self.add_node(node)
        for f in node.friends:
            print("addr: %s %s" % (f["addr"], f["flags"]))
            if "noaddr" in f["flags"] or "disconnected" in f["flags"] or "fail" in f["flags"]:
                continue
            fnode = createRedisNode(f["addr"])
            self.add_node(fnode)
        self.populate_nodes_replicas_info()
        return

    # This function is called by load_cluster_info_from_node in order to
    # add additional information to every node as a list of replicas.
    def populate_nodes_replicas_info(self):
        # Start adding the new field to every node.
        for n in self.nodes:
            n.info["replicas"] = []

        # Populate the replicas field using the replicate field of slave
        # nodes.
        for n in self.nodes:
            if n.info["replicate"]:
                master = self.get_node_by_name(n.info["replicate"])
                if master:
                    master.info["replicas"].append(n)
                else:
                    print("*** WARNING: %s claims to be slave of unknown node ID %s." % (n, n.info["replicate"]))

    # Given a list of source nodes return a "resharding plan"
    # with what slots to move in order to move "numslots" slots to another
    # instance.
    def compute_reshard_table(self, sources,numslots):
        moved = []
        # Sort from bigger to smaller instance, for two reasons:
        # 1) If we take less slots than instances it is better to start
        #    getting from the biggest instances.
        # 2) We take one slot more from the first instance in the case of not
        #    perfect divisibility. Like we have 3 nodes and need to get 10
        #    slots, we take 4 from the first, and 3 from the rest. So the
        #    biggest is always the first.
        sources = sorted(sources, key=lambda x: -x.slots_length())
        source_tot_slots = 0
        for obj in sources:
            source_tot_slots += obj.slots_length()
        i = 0
        for s in sources:
            # Every node will provide a number of slots proportional to the
            # slots it has assigned.
            n = float(numslots)/source_tot_slots*s.slots_length()
            if i == 0:
                n = ceil(n)
            else:
                n = floor(n)
            slots_list = sorted(s.get_slots())[:n]
            for slot in slots_list:
                if len(moved) < numslots:
                    moved.append({"source":s, "slot": slot})
        return moved

    # Move slots between source and target nodes using MIGRATE.
    #
    # Options:
    # :fix     -- We are moving in the context of a fix. Use REPLACE.
    # :cold    -- Move keys without opening slots / reconfiguring the nodes.
    def move_slot(self, source, target, slot, opt):
        o = {"pipeline": self.pipeline}
        o.update(opt)

        # We start marking the slot as importing in the destination node,
        # and the slot as migrating in the target host. Note that the order of
        # the operations is important, as otherwise a client may be redirected
        # to the target node that does not yet know it is importing this slot.
        print("Moving slot %s from %s to %s: " % (slot, source, target))

        if not "cold" in o or not o["cold"]:
            target.r.execute("cluster","setslot",slot,"importing",source.full_name())
            source.r.execute("cluster","setslot",slot,"migrating",target.full_name())

        # Migrate all the keys from source to target using the MIGRATE command
        while True:
            keys = source.r.execute("cluster","getkeysinslot",slot,o["pipeline"])
            if len(keys) == 0:
                break
            for index in range(len(keys)):
                keys[index] = keys[index].decode()
            try:
                source.r.execute("migrate",target.info["host"],target.info["port"],"",0,self.timeout,"keys", *keys)
            except Exception as e:
                if "fix" in o and str(e).find("BUSYKEY") != -1:
                    print("*** Target key exists. Replacing it for FIX.")
                    source.r.execute("migrate",target.info["host"],target.info["port"],"",0,self.timeout,"replace","keys", *keys)
                else:
                    print("[ERR] Calling MIGRATE: %s" % e)
                    exit(1)

        # Set the new node as the owner of the slot in all the known nodes.
        if "cold" not in o or not o["cold"]:
            for n in self.nodes:
                if n.has_flag("slave"):
                    continue
                n.r.execute("cluster", "setslot", slot, "node", target.full_name())

        # Update the node logical config
        if slot in source.info["slots"]:
            source.info["slots"].remove(slot)
        if slot in source.info["new_slots"]:
            source.info["new_slots"].remove(slot)
        target.info["slots"].add(slot)
        return

    def show_nodes(self):
        for n in self.nodes:
            print(n.info_string())
        return


    # This is an helper function for create_cluster_cmd that verifies if
    # the number of nodes and the specified replicas have a valid configuration
    # where there are at least three master nodes and enough replicas per node.
    def check_create_parameters(self):
        masters = len(self.nodes)/(self.replicas+1)
        if masters < 3:
            print("*** ERROR: Invalid configuration for cluster creation.")
            print("*** Redis Cluster requires at least 3 master nodes.")
            print("*** This is not possible with %d nodes and %d replicas per node." % (len(self.nodes), self.replicas))
            print("*** At least %d nodes are required." % (3 * (self.replicas+1)))
            exit(1)
        return

    def create_cluster_cmd(self, addrs):
        print(">>> Creating cluster")
        for addr in addrs:
            node = createRedisNode(addr, new=True)
            self.add_node(node)

        self.check_create_parameters()
        print(">>> Performing hash slots allocation on %d nodes..." % len(self.nodes))
        self.alloc_slots()
        self.show_nodes()
        self.flush_nodes_config()
        print(">>> Nodes configuration updated")
        print(">>> Assign a different config epoch to each node")
        self.assign_config_epoch()
        print(">>> Sending CLUSTER MEET messages to join the cluster")
        self.join_cluster()
        # Give one second for the join to start, in order to avoid that
        # wait_cluster_join will find all the nodes agree about the config as
        # they are still empty with unassigned slots.
        time.sleep(1)
        self.wait_cluster_join()
        self.flush_nodes_config() # Useful for the replicas
        # Reset the node information, so that when the
        # final summary is listed in check_cluster about the newly created cluster
        # all the nodes would get properly listed as slaves or masters
        self.reset_nodes()
        self.load_cluster_info_from_node(addrs[0])
        self.check_cluster()
        self.cluster_addr = addrs[0]

        return

    def check_cluster_cmd(self):
        print(">>> Checking cluster")
        self.check_config_consistency()

    def addnode_cluster_cmd(self, node_addr, slave=False, master_id=None):
        print(">>> Adding node %s to cluster" % node_addr)

        # If --master-id was specified, try to resolve it now so that we
        # abort before starting with the node configuration.
        if slave:
            if master_id:
                master = self.get_node_by_name(master_id)
                if not master:
                    print("[ERR] No such master ID %s" % master_id)
                    exit(1)
            else:
                master = self.get_master_with_least_replicas()
                print("Automatically selected master %s" % master)

        # Add the new node
        new = createRedisNode(node_addr, new=True)
        first = list(self.nodes)[0].info
        self.add_node(new)

        # Send CLUSTER MEET command to the new node
        print(">>> Send CLUSTER MEET to node %s to make it join the cluster." % new)
        new.r.execute("cluster", "meet", first["host"], first["port"])

        # Additional configuration is needed if the node is added as
        # a slave.
        self.wait_cluster_join()
        if slave:
            print(">>> Configure node as replica of %s." % master.full_name())
            time.sleep(1)
            new.r.execute("cluster", "replicate", master.full_name())
            self.wait_cluster_join()

        self.reload_info()
        print(">>>[OK] New node added correctly.")
        return

    def rebalance_cluster_cmd(self, threshold=None):
        if not threshold:
            threshold = RebalanceDefaultThreshold

        # Only consider nodes we want to change
        sn = []
        for n in self.nodes:
            if n.has_flag("master"):
                print("master node: %s" % n)
                sn.append(n)

        # Assign a weight to each node, and compute the total cluster weight.
        # Calculate the slots balance for each node. It's the number of
        # slots the node should lose (if positive) or gain (if negative)
        # in order to be balanced.
        threshold_reached = False
        expected = int(float(ClusterHashSlots) / len(sn))
        for n in self.nodes:
            slots_length = n.slots_length()
            if n.has_flag("master"):
                n.info["balance"] = slots_length - expected
                # Compute the percentage of difference between the
                # expected number of slots and the real one, to see
                # if it's over the threshold specified by the user.
                if not threshold_reached:
                    perc = slots_length * threshold
                    if n.info["balance"] < 0:
                        if n.info["balance"] < -perc:
                            threshold_reached = True
                    else:
                        if n.info["balance"] > perc:
                            threshold_reached = True

        if not threshold_reached:
            print("*** No rebalancing needed! All nodes are within the %d threshold." % threshold)
            return

        # Because of rounding, it is possible that the balance of all nodes
        # summed does not give 0. Make sure that nodes that have to provide
        # slots are always matched by nodes receiving slots.
        total_balance = 0
        for n in sn:
            total_balance += n.info["balance"]
        while total_balance > 0:
            for n in sn:
                if total_balance <= 0:
                    break
                if n.info["balance"] < 0:
                    n.info["balance"] -= 1
                    total_balance -= 1

        # Sort nodes by their slots balance.
        sn = sorted(sn, key=lambda x: x.info["balance"])
        print(">>> Rebalancing across %d nodes" % len(sn))

        # Now we have at the start of the 'sn' array nodes that should get
        # slots, at the end nodes that must give slots.
        # We take two indexes, one at the start, and one at the end,
        # incrementing or decrementing the indexes accordingly til we
        # find nodes that need to get/provide slots.
        dst_idx = 0
        src_idx = len(sn) - 1

        while dst_idx < src_idx:
            dst = sn[dst_idx]
            src = sn[src_idx]
            numslots = 0
            if dst.info["balance"] + src.info["balance"] > 0:
                numslots = -dst.info["balance"]
            else:
                numslots = src.info["balance"]

            if numslots > 0:
                print("Moving %d slots from %s to %s" % (numslots, src, dst))

                # Actaully move the slots.
                reshard_table = self.compute_reshard_table([src], numslots)
                if len(reshard_table) != numslots:
                    print("*** Assertio failed: Reshard table != number of slots")
                    exit(1)

                for e in reshard_table:
                    self.move_slot(e["source"], dst, e["slot"], {})
                    print("#")

            # Update nodes balance.
            dst.info["balance"] += numslots
            src.info["balance"] -= numslots
            if dst.info["balance"] == 0:
                dst_idx += 1
            if src.info["balance"] == 0:
                src_idx -= 1

    def reshard_cluster_cmd(self, target_name, source_name_list, number, o={}):
        opt = {
            "pipeline" : MigrateDefaultPipeline,
            "timeout": self.timeout
        }
        opt.update(o)

        # Get the target instance
        target = self.get_node_by_name(target_name)
        if not target or target.has_flag("slave"):
            print("*** The specified node is not known or not a master, please retry.")
            exit(1)

        if len(source_name_list) == 0:
            print("*** No source nodes given, operation aborted")
            return

        # Get the source instances
        sources = []
        if source_name_list[0] != "all":
            for node_id in source_name_list:
                src = self.get_node_by_name(node_id)
                if not src or src.has_flag("slave"):
                    print("*** The specified node is not known or is not a master, please retry.")
                    exit(1)
                # Check if the destination node is the same of any source nodes.
                if src.full_name() == target.full_name():
                    print("*** Target node is also listed among the source nodes!")
                    exit(1)
                sources.append(src)
        else:
            # Handle soures == all.
            for n in self.nodes:
                if n.full_name() != target.full_name() and n.has_flag("master"):
                    sources.append(n)

        print("\nReady to move #{numslots} slots.")
        print("  Source nodes:")
        for s in sources:
            print("    " + s.info_string())
        print("  Destination node:")
        print("    " + target.info_string())
        reshard_table = self.compute_reshard_table(sources, number)
        print("  Resharding plan:")
        for e in reshard_table:
            self.move_slot(e["source"], target, e["slot"], {"pipeline":opt['pipeline']})

    def delnode_cluster_cmd(self, node_name):
        print(">>> Removing node %s from cluster " % node_name)

        # Check if the node exists and is not empty
        node = self.get_node_by_name(node_name)
        if not node:
            print("[ERR] No such node ID %s" % node_name)
            exit(1)

        if node.slots_length() != 0:
            print("[ERR] Node %s is not empty! Reshard data away and try again." % node)
            exit(1)

        # Send CLUSTER FORGET to all the nodes but the node to remove
        print(">>> Sending CLUSTER FORGET messages to the cluster...")
        for n in self.nodes:
            if n == node:
                continue
            if n.info["replicate"] and n.info["replicate"] == node_name:
                # Reconfigure the slave to replicate with some other node
                master = self.get_master_with_least_replicas(node)
                print(">>> %s as replica of %s" % (n, master))
                n.r.execute("cluster", "replicate", master.full_name())
            n.r.execute("cluster", "forget", node_name)

        self.reload_info()
        print(">>>[OK] New node deleted correctly.")

    def auto_delnode_cluster_cmd(self, node_name, o={}):
        print(">>> Auto removing node %s from cluster " % node_name)
        opt = {
            "pipeline" : MigrateDefaultPipeline,
            "timeout": self.timeout
        }
        opt.update(o)

        # Check if the node exists and is not empty
        node = self.get_node_by_name(node_name)
        if not node:
            print("[ERR] No such node ID %s" % node_name)
            exit(1)

        # remove slots
        if node.has_flag("master"):
            slots = node.get_slots()
            slots_length = len(slots)
            if slots_length != 0:
                # some other master
                left_master = []
                for n in self.nodes:
                    if n.has_flag("master"):
                        if n == node:
                            continue
                        left_master.append(n)
                for n in left_master:
                    n.info["balance"] = int(slots_length / len(left_master))
                n.info["balance"] += slots_length - n.info["balance"] * len(left_master)
                slots = sorted(list(slots))
                for n in left_master:
                    print("Resharding plan for %s:" % n)
                    for index in range(n.info["balance"]):
                        self.move_slot(node, n, slots[index], {"pipeline":opt['pipeline']})
                    slots = slots[n.info["balance"]:]

        # Send CLUSTER FORGET to all the nodes but the node to remove
        print(">>> Sending CLUSTER FORGET messages to the cluster...")
        for n in self.nodes:
            if n == node:
                continue
            if n.info["replicate"] and n.info["replicate"] == node_name:
                # Reconfigure the slave to replicate with some other node
                master = self.get_master_with_least_replicas(node)
                print(">>> %s as replica of %s" % (n, master))
                n.r.execute("cluster", "replicate", master.full_name())
            n.r.execute("cluster", "forget", node_name)

        self.reload_info()
        print(">>>[OK] New node deleted correctly.")


if __name__ == "__main__":

    addrs =["127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003", "127.0.0.1:7004", "127.0.0.1:7005"]

    # Create
    control = RedisControl()
    control.create_cluster_cmd(addrs)

    # Check
    control.check_cluster_cmd()
    control = RedisControl(addrs[0])
    control.check_cluster_cmd()
    time.sleep(1)

    # load data
    from rediscluster import StrictRedisCluster
    import uuid
    startup_nodes = [{"host": "127.0.0.1", "port": "7000"}]
    rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    num = 100
    for index in range(100 * num):
        if index % num == 0:
            print("loading .. %d" % int(index / num))
        rc.set(uuid.uuid4(), index)

    # add master
    control.addnode_cluster_cmd("127.0.0.1:7006")

    # add slave
    new = ClusterNode("127.0.0.1:7006")
    new.load_info()
    control.addnode_cluster_cmd("127.0.0.1:7007", slave=True, master_id=new.full_name())
    time.sleep(10)

    # rebalance
    control.rebalance_cluster_cmd()

    # reshard
    new6 = ClusterNode("127.0.0.1:7006")
    new6.load_info()
    new7 = ClusterNode("127.0.0.1:7007")
    new7.load_info()


    control.delnode_cluster_cmd(new7.full_name())
    control.auto_delnode_cluster_cmd(new6.full_name())
    exit(0)
