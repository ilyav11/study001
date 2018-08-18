import ipaddress
import consistent as cs
import time
import logging
import random



def delete_route(ch: cs.ConsistentHash, route):
    p = cs.Prefix()
    p.set_prefix(route)

    r = cs.Route(p, set())

    ch.del_route(r)

def dump_ch(ch):
    print("\n===\nConsistent ", ch)

    print("\nRoutes\n", ch.Routes)
    print("\nDesiredContainers\n", ch.DesiredContainers)
    print("\nActualContainers\n", ch.ActualContainers)

    print("\nSDK\n", ch.SdkObject)

    print("\n", " ".join(str(s) for s in ch.Routes.prefixes()))

base_nh = "192.1.1.1"
base_net  = "172.1.1.0"

next_hop_set = set()
prefix_set = set()

test_route_set = set()


def add_routes(ch: cs.ConsistentHash):

    for r in test_route_set:
        ch.add_route(r)


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

    for i in prefix_set:
        p = cs.Prefix()
        p.set_prefix(i)

        s = set()

        r = cs.Route(p, s)

        test_route_set.add(r)



def generate_route_set(cycle):
    r: cs.Route
    for r in test_route_set:
        nh_set = next_hop_set - r.nh_set

        assert len(nh_set) > 0

        idx = random.randint(0, len(nh_set)-1)

        nh = list(nh_set)[idx]

        r.nh_set.add(nh)



def generate_run(routes, next_hops):

    generate_data(routes, next_hops)

#    ch = cs.ConsistentHash(debug_level = logging.DEBUG)
    ch = cs.ConsistentHash()
    ch.run()
    ch.set_admin_state(True)

    for i in range(next_hops):
        generate_route_set(i)

        print(cs.pSet(test_route_set))
        
        add_routes(ch)

        dump_ch(ch)

        time.sleep(5)

    time.sleep(10)

    dump_ch(ch)

    ch.stop()




generate_run(200,5)   