#!/usr/bin/env python
# Author: catlittlechen@gmail.com
# encoding: utf-8

from pyredis import Client


class ClusterNode():

    def __init__(self, addr):
        s = str(addr).split(":")
        if len(s) < 2:
            print("Invalid IP or Port (given as %s) - use IP:Port format" % addr)
            exit(1)

        # connection
        self.r = None
        self.info = {}

        # addr
        self.info["host"] = ":".join(s[:-1])
        if s[-1].find('@') != -1:
            port = s[-1].split('@')
            self.info["port"] = int(port[0])
        else:
            self.info["port"] = int(s[-1])

        # data slot set
        self.info["slots"] = set()
        self.info["new_slots"] = set()

        # record slot-migrate message
        self.info["migrating"] = {}
        self.info["importing"] = {}

        # master-id
        self.info["replicate"] = None
        # slave-id list
        self.info["replicas"] = []

        self.dirty = False
        self.friends = []

    def __str__(self):
        return self.info["host"] + ":" + str(self.info["port"])

    def connect(self):
        if self.r != None:
            return
        print("Connecting to node %s" % self)
        try:
            self.r = Client(host=self.info["host"], port=self.info["port"])
            self.r.ping()
        except Exception as e:
            print("[ERR] Sorry, can't connect to node %s" % self)
            print(e)
            exit(1)
        print("OK")

    def assert_cluster(self):
        if self.r == None:
            self.connect()

        info = self.r.execute("info", "cluster").decode()
        info_list = str(info).split("\r\n")
        info = {}
        for obj in info_list[1:]:
            s = obj.split(":")
            info[s[0]] = s[1]
        if "cluster_enabled" not in info or info["cluster_enabled"] == "0":
            print("[ERR] Node %s is not configured as a cluster node" % self)
            exit(-1)

    # info
    def has_flag(self, flag):
        return flag in self.info["flags"]

    def replicas_length(self):
	    return len(self.info["replicas"])

    def slots_length(self):
	    return len(self.info["slots"]) + len(self.info["new_slots"])

    def full_name(self):
	    return self.info["name"]

    def dbsize(self):
        data = r.execute("info", "Keyspace").decode().split("\r\n")
        if len(data) < 2:
            return 0
        return int(data[1].split(":")[1].split(",")[0].split("=")[1])

    # cluster info
    def load_info(self):
        if self.r == None:
            self.connect()

        nodes = self.r.execute("cluster", "nodes").decode().split("\n")
        for node in nodes:
            array = node.split(" ")

            slots = []
            if len(array) > 8:
                slots = array[8:]

            info = {
                "name": array[0],
                "addr": array[1],
                "flags": set(array[2].split(",")),
                "replicate": array[3],
                "ping_sent": int(array[4]),
                "ping_recv": int(array[5]),
                "link_status": array[6],
            }
            if info["replicate"] == "-":
                info["replicate"] = None

            if "myself" not in info["flags"]:
                self.friends.append(info)
                continue

            self.info.update(info)
            self.info["slots"] = set()
            for s in slots:
                if s[0] == '[':
                    if str(s).find("->-") != -1:
                        slot_dst = str(s[1:-1]).split("->-")
                        self.info["migrating"][int(slot_dst[0])] = slot_dst[1]
                    elif str(s).find("-<-") != -1:
                        slot_src = str(s[1:-1]).split("-<-")
                        self.info["importing"][int(slot_src[0])] = slot_src[1]
                elif str(s).find("-") != -1:
                    start_stop = str(s).split("-")
                    self.add_slots(int(start_stop[0]), int(start_stop[1]))
                else:
                    self.add_slots(int(s), int(s))
            self.dirty = False
            cluster_info = self.r.execute("cluster", "info").decode().split("\n")
            for ci in cluster_info:
                kv = ci.split(":")
                if str(kv[0]) != "cluster_state":
                    self.info[kv[0]] = int(kv[1].rstrip("\r"))
                else:
                    self.info[kv[0]] = kv[1].rstrip("\r")
        return

    def add_slots(self, start, stop):
        for index in range(start, stop+1):
            self.info["new_slots"].add(index)
        self.dirty = True
        return

    def has_slot(self, slot):
        if slot in self.info["slots"]:
            return True
        if slot in self.info["new_slots"]:
            return True
        return False

    def get_slots(self):
        return self.info["slots"].union(self.info["new_slots"])

    def has_keys(self, slot):
        return len(self.r.execute("cluster", "getkeysinslot", slot, 1)) > 0

    def set_as_replicate(self, node_id):
        self.info["replicate"] = node_id
        self.dirty = True
        return

    def flush_node_config(self):
        if self.dirty is False:
            return
        if self.info["replicate"]:
            try:
                self.r.execute("cluster", "replicate", self.info["replicate"])
            except:
                return
        else:
            self.info["slots"] = self.info["slots"].union(self.info["new_slots"])
            new_slots = list(self.info["new_slots"])
            self.r.execute("cluster", "addslots", *new_slots)
            self.info["new_slots"] = set()
        self.dirty = False

    def info_string(self):
        # We want to display the hash slots assigned to this node
        # as ranges, like in: "1-5,8-9,20-25,30"
        #
        # Note: this could be easily written without side effects,
        # we use 'slots' just to split the computation into steps.

        # First step: we want an increasing array of integers
        # for instance: [1,2,3,4,5,8,9,20,21,22,23,24,25,30]
        slots = sorted(list(self.info["slots"].union(self.info["new_slots"])))
        slots_list = []
        if len(slots) != 0:
            first = slots[0]
            del slots[0]
            last = first
            for tmp in slots:
                if tmp != last + 1:
                    if first == last:
                        slots_list.append(str(first))
                    else:
                        slots_list.append("%d-%d" % (first, last))
                    first = tmp
                last = tmp

            if first == last:
                slots_list.append(str(first))
            else:
                slots_list.append("%d-%d" % (first, last))


        # Now our task is easy, we just convert ranges with just one
        # element into a number, and a real range into a start-end format.
        # Finally we join the array using the comma as separator.
        slot_str = ",".join(slots_list)

        role = "S"
        if self.has_flag("master"):
            role = "M"

        if self.info["replicate"] and self.dirty:
            iS = "S: %s %s" % (self.info["name"], self)
        else:
            iS = "%s: %s %s\n   slots:%s (%d slots) "  % (
                role, self.info["name"], self,
                slot_str, self.slots_length()
            )
            flags = self.info["flags"].copy()
            if "myself" in flags:
                flags.remove("myself")
            iS += ",".join(list(flags))
        if self.info["replicate"]:
            iS += "\n   replicates " + self.info["replicate"]
        elif self.has_flag("master") and len(self.info["replicas"]) != 0:
            iS += "\n   %d additional replica(s)" % len(self.info["replicas"])
        return iS


    # Return a single string representing nodes and associated slots.
    # TODO: remove slaves from config when slaves will be handled
    # by Redis Cluster.
    def get_config_signature(self):
        config = []
        data = self.r.execute("cluster", "nodes").decode().split("\n")
        for l in data:
            s = l.split(" ")
            if len(s) > 8:
                slots = []
                for index in range(8, len(s)):
                    if s[index][0] != '[':
                        slots.append(s[index])
                if len(slots) > 0:
                    config.append(s[0] + ":" + ",".join(slots))
        return "|".join(sorted(config))

    def assert_empty(self):
        info = set(self.r.execute("cluster", "info").decode().split("\r\n"))
        db0 = str(self.r.execute("info")).find("db0:") != -1
        if "cluster_known_nodes:1" not in info:
            print("[ERR] Node %s is not empty. the node already knows other nodes (check with CLUSTER NODES)." % self)
        if db0:
            print("[ERR] Node %s is not empty. the node contains some key in database 0." % self)
            exit(1)
        return


if __name__ == "__main__":
    r = ClusterNode("127.0.0.1:7007")
    r.connect()
    r.assert_cluster()
    r.assert_empty()
    #r.load_info()
    #print(r.get_config_signature())
