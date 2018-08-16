import consistent as cs



p = cs.Prefix()
p.set_prefix("192.168.1.1/24")

nh1 = cs.Nexthop("10.0.0.1")
nh2 = cs.Nexthop("10.0.0.2")
s = set([nh1, nh2])

r = cs.Route(p, s)

ch = cs.ConsistentHash()

ch.add_route(r)


print("\nDesiredContainers\n", "\n\n".join(str(dc) for dc in cs.DesiredContainers))


p.set_prefix("172.1.1.1/24")

r = cs.Route(p,s)

ch.add_route(r)

print("\nDesiredContainers\n", "\n\n".join(str(dc) for dc in cs.DesiredContainers))

p.set_prefix("192.168.1.2/24")

nh1 = cs.Nexthop("10.0.0.3")
nh2 = cs.Nexthop("10.0.0.2")
s = set([nh1, nh2])

r = cs.Route(p, s)

ch.add_route(r)

print("\nDesiredContainers\n", "\n\n".join(str(dc) for dc in cs.DesiredContainers))

p.set_prefix("172.1.1.2/24")

r = cs.Route(p,s)

ch.add_route(r)

print("\n===\nConsistent ", ch)

print("\nRoutes\n", cs.Routes)
print("\nDesiredContainers\n", "\n\n".join(str(dc) for dc in cs.DesiredContainers))
print("\nActualContainers\n", cs.ActualContainers)

print("\n", " ".join(str(s) for s in cs.Routes.prefixes()))
