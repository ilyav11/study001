import ipaddress
import consistent as cs
import time
import logging
import random
import copy



def delete_route(ch: cs.ConsistentHash, route):
    p = cs.Prefix()
    p.set_prefix(route)

    r = cs.Route(p, set())

    ch.del_route(r)

def dump_ch(ch):
    print("\n===\nConsistent ", ch)

#    print("\nRoutes\n", ch.Routes)
    print("\nDesiredContainers\n", ch.DesiredContainers)
    print("\nActualContainers\n", ch.ActualContainers)

    print("\nSDK\n", ch.SdkObject)

#    print("\n", " ".join(str(s) for s in ch.Routes.prefixes()))

base_nh = "192.1.1.1"
base_net  = "172.1.1.0"

next_hop_set = set()
prefix_set = set()

#test_dictionary = {}

test_route_list = []

test_full_route_list = []


def simulate_fat_tree_converge(ch: cs.ConsistentHash):

    count = 0
    for r in test_route_list:
        ch.add_route(r)

        count += 1

        if count % 200 == 0:
            print(".")
            time.sleep(1)


def generate_data(routes, next_hops):

    global next_hop_set
    global prefix_set

    next_hop_set = set()

    for i in range(next_hops):

        addr = ipaddress.ip_address(base_nh) + random.randint(256, 256+10000)
        nh = ipaddress.ip_address(addr)

        next_hop_set.add(nh)

    for i in range(routes):
        addr = ipaddress.ip_address(base_net) + random.randint(256, 256+100000)
        route = ipaddress.ip_network(str(ipaddress.ip_address(addr)) + "/24",strict = False)

        prefix_set.add(route)

def generate_full_route():

    test_full_route_list = []
    
    for prefix in prefix_set:
        
        p = cs.Prefix()
        p.set_prefix(prefix)
        s = set()

        for i in next_hop_set):
            s.add(nh)
        
            r = cs.Route(p, s)
            test_full_route_list[prefix] = r


def generate_fat_tree_converge():

    test_dictionary = {}

    for prefix in prefix_set:
        p_list = []

        p_nh = copy.copy(next_hop_set)

        p = cs.Prefix()
        p.set_prefix(prefix)
        s = set()

        for i in range(len(next_hop_set)):
            idx = random.randint(0, len(p_nh) - 1)
            nh = list(p_nh)[idx]

            s.add(nh)
            p_nh.remove(nh)

            r = cs.Route(p, s)
            p_list.append(r)

        test_dictionary[prefix] = p_list

    
#    print(", ".join("{}: [{}] ".format(str(a),", ".join(str(c) for c in b))  for a,b in test_dictionary.items()))

    while True:

        if len(test_dictionary.keys()) == 0:
            break

        idx = random.randint(0, len(test_dictionary.keys()) - 1 )

        key = list(test_dictionary.keys())[idx]

        l = test_dictionary[key]
        
        if len(l) == 0:
            del test_dictionary[key]
            continue

        r = l[0]

        test_route_list.append(r)
        l.remove(r)

        if len(l) == 0:
            del test_dictionary[key]

#    print(cs.pSet(test_route_list))


def generate_run(routes, next_hops):

    print("Generating data...\n")
    generate_data(routes, next_hops)

    print("Generating fat tree converge")
    generate_fat_tree_converge()

    print("Generate full route list")
    generate_full_route()

 #   ch = cs.ConsistentHash(debug_level = logging.DEBUG)
    ch = cs.ConsistentHash()
    ch.run()
    ch.set_admin_state(True)
  
    print("Simulate fat tree converge...\n")
    simulate_fat_tree_converge(ch)

#    dump_ch(ch)

    time.sleep(10)

    dump_ch(ch)

    ch.stop()




generate_run(200,5)   