import ipaddress
import enum
import copy


class pSet:
    def __init__(self, s):
        self._s = s
    
    @property
    def s(self):
        return self._s

    @s.setter
    def s(self, s):
        self._s = s

    def __str__(self):
        return "{" +  ", ".join(str(s) for s in self._s) + "}"

    def __iter__(self):
        for s in self._s:
            yield s

class Nexthop :

    def __init__(self, ipaddr):
        self._ipaddr = ipaddress.ip_address(ipaddr)

    
    def set_nh(self, ipaddr):
        self._ipaddr = ipaddr


    def __str__(self):
        return str(self._ipaddr)


    @property
    def ipaddress(self):
        return self._ipaddr


class Prefix:
    def __init__(self):
        self._prefix = ipaddress.ip_address(0)

    def set_prefix(self, prefix):
        self._prefix = ipaddress.ip_network(prefix, strict = False)

    def __str__(self):
        return str(self._prefix)

    def __eq__(self, other):
        return self._prefix == other._prefix


    @property
    def network(self):
        return self._prefix.network_address

    @property
    def mask(self):
        return self._prefix.netmask

    @property
    def hashable(self):
        return self._prefix.with_prefixlen

class Route:
    def __init__(self, prefix, nhset):
        
        self._prefix = copy.copy(prefix)
        self._nhset = copy.copy(nhset)
        self._dc = None


    def set_prefix(self, prefix):
        self._prefix = prefix

    def set_nhset(self, nhset):
        self._nhset = nhset

    @property
    def prefix(self):
        return self._prefix

    @property
    def nh_set(self):
        return self._nhset
    
    @property
    def desired_container(self):
        return self._dc

    def __str__(self):
        return str(self._prefix) + "\t-->\t{" + ", ".join(str(s) for s in self._nhset) + "}" + "(0x{:02X})".format(id(self._dc))

class RouteContainer:
    def __init__(self):
        self._d = {}
    
    def add(self, r: Route):
        self._d[r.prefix.hashable] = r

    def remove(self, r: Route):
        self._d.pop(r.prefix.hashable)
    
    def __iter__(self):
        for s in self._d.keys():
            yield self._d[s]

    def __str__(self):
        return "{" + ", ".join(str(self._d[s]) for s in self._d.keys()) + "}"

    def prefixes(self):
        s: Route
        for s in self._d.keys():
            yield self._d[s].prefix

    def __getitem__(self, r:Prefix):
        if r in self.prefixes():
            return self._d[r.hashable]
        else:
            return None

    def __setitem__(self, idx: Prefix, r: Route):
        self._d[idx.hashable] = r

    def __delitem__(self, r: Prefix):
        self._d.pop(r.hashable)

Routes = RouteContainer()


class ActualContainer:
    def __init__(self):
        self._dc = None
        self._resolved = False
        self._nh_set = set()

    @property
    def desired_container(self):
        return self._dc

    @property
    def resolved(self):
        return self._resolved

    @property
    def nh_set(self):
        return self._nh_set


ActualContainers = pSet(set())


class DesiredContainer:

    class State(enum.Enum):
        RESOLVED = 1
        FAILED = 2
        PARTIAL = 3

    def __init__(self):
        self._current_state = self.State.FAILED
        self._nh_set = pSet(set())
        self._ac = None
        self._child_set = set()
        self._father = None
        self._ref_count = 0

    @property
    def current_state(self):
        return self._current_state

    @property
    def nh_set(self):
        return self._nh_set.s

    @nh_set.setter
    def nh_set(self, nh_set):
        self._nh_set = pSet(nh_set)

    @property
    def active_container(self):
        return self._ac

    @property
    def child_set(self):
        return self._child_set

    @property
    def father(self):
        return self._father

    @property
    def ref_count(self):
        return self._ref_count

    @ref_count.setter
    def ref_count(self, ref_count: int):
        self._ref_count = ref_count

    def __str__(self):
        return "id: " + "0x{:02X}".format(id(self)) + "\n" + "\n".join(str(a) + ": " + str(b) for a,b in vars(self).items())

DesiredContainers = pSet(set())

class SDK:
    def __init__(self):
        pass

    def SDKProgramRoute(self, route: Route):
        pass

SdkObject = SDK() 

_current_time: int = 0

class ConsistentHash:

    class SystemState(enum.Enum):
        STABLE = 1
        NON_STABLE = 2

    class SystemConsistent(enum.Enum):
        CONSISTENT = 1
        NON_CONSISTENT = 2

    _long_period_of_time = 600 #seconds

    _periodic_timer = 30 #seconds
    
    def __init__(self):
        self._system_consistent = self.SystemConsistent.CONSISTENT
        self._system_stable = self.SystemState.STABLE
        self._last_stable = _current_time


    
    def add_route(self, route: Route):
        if route.prefix in Routes.prefixes():
            self._change_route(route)
        else:
            self._new_route(route)

    def _new_route(self, route: Route):

        global Routes
        global DesiredContainers
        global SdkObject

        newRoute = Route(route.prefix, route.nh_set)

        Routes.add(newRoute)

        l_dc = [dc for dc in DesiredContainers if dc.nh_set == newRoute.nh_set]
        
        if l_dc:
            if len(l_dc)!= 1:
                raise AssertionError
            dc = l_dc[0]
            newRoute.dc = dc
            dc.ref_count += 1
        else:
            dc = DesiredContainer()
            newRoute.dc = dc
            dc.ref_count = 1
            dc.nh_set = newRoute.nh_set
            self._allocate_new_ac(dc)
            DesiredContainers.s.add(dc)
        if dc.State != DesiredContainer.State.FAILED:
            SdkObject.SDKProgramRoute(newRoute)

        self._periodic()


    def _change_route(self, route: Route):
        currR = Routes[route.prefix]

        print(currR, route)
        
    def del_route(self, route: Route):
        pass


    def _periodic(self):
        pass

    def periodic_tick(self):
        global _current_time

        _current_time += self._periodic_timer

    def _allocate_new_ac(self, dc: DesiredContainer):
        pass

    def __str__(self):
        return str(vars(self))




if __name__ == "__main__":

    p = Prefix()
    p.set_prefix("192.168.1.1/24")

    nh1 = Nexthop("10.0.0.1")
    nh2 = Nexthop("10.0.0.2")
    s = set([nh1, nh2])

    r = Route(p, s)

    ch = ConsistentHash()

    ch.add_route(r)

    p.set_prefix("172.1.1.1/24")

    r = Route(p,s)

    ch.add_route(r)

    p.set_prefix("192.168.1.2/24")

    nh1 = Nexthop("10.0.0.3")
    nh2 = Nexthop("10.0.0.2")
    s = set([nh1, nh2])

    r = Route(p, s)

    ch.add_route(r)

    p.set_prefix("172.1.1.2/24")

    r = Route(p,s)

    ch.add_route(r)

    print("Consistent ", ch)
    print("Routes\n", "\n".join(str(r) for r in Routes))
    print("DesiredContainers\n", "\n".join(str(dc) for dc in DesiredContainers))
    print("ActualContainers\n", ActualContainers)

    print(" ".join(str(s) for s in Routes.prefixes()))


