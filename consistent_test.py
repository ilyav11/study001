import ipaddress
import consistent as cs


def add_route(ch: cs.ConsistentHash, route, ns_list):
    p = cs.Prefix()
    p.set_prefix(route)

    s = set()

    for l in ns_list:
        nh = cs.Nexthop(l)
        s.add(nh)

    r = cs.Route(p, s)

    ch.add_route(r)

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

    print("\n", " ".join(str(s) for s in ch.Routes.prefixes()))



ch = cs.ConsistentHash()

add_route(ch, "192.1.1.1/24", ["10.0.0.1", "10.0.0.2" , "10.0.0.3" ])
add_route(ch, "172.1.1.1/24",   ["10.0.0.1", "10.0.0.2",  "10.0.0.3" ])
add_route(ch, "173.1.1.1/24",   ["10.0.0.1", "10.0.0.2",  "10.0.0.3" ])

add_route(ch, "192.1.1.1/24", ["10.0.0.1", "10.0.0.2" ])
add_route(ch, "172.1.1.1/24",   ["10.0.0.1", "10.0.0.2" ])

add_route(ch, "172.1.1.1/24",   ["10.0.0.1" ])

# delete_route(ch, "192.1.1.1/24" )

dump_ch(ch)