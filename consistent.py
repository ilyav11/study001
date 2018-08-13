import ipaddress
import enum



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

class Route:
    def __init__(self, prefix, nhset):
        
        self._prefix = prefix
        self._nhset = nhset
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
        return str(self._prefix) + "-->{" + ", ".join(str(s) for s in self._nhset) + "}"

Routes = set()


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


ActualContainers = set()


class DesiredContainer:

    class State(enum.Enum):
        RESOLVED = 1
        FAILED = 2
        PARTIAL = 3

    def __init__(self):
        self._current_state = self.State.FAILED
        self._nh_set = set()
        self._ac = None
        self._child_set = set()
        self._father = None
        self._ref_count = 0

    @property
    def current_state(self):
        return self._current_state

    @property
    def nh_set(self):
        return self._nh_set

    @nh_set.setter
    def nh_set(self, nh_set):
        self._nh_set = nh_set

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
        return str(vars(self))

DesiredContainers = set()

class Prefix:
    def __init__(self):
        self._prefix = ipaddress.ip_address(0)

    def set_prefix(self, prefix):
        self._prefix = ipaddress.ip_network(prefix, strict = False)

    def __str__(self):
        return str(self._prefix)


    @property
    def network(self):
        return self._prefix.network_address

    @property
    def mask(self):
        return self._prefix.netmask


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
        if route in Routes:
            self._change_route(route)
        else:
            self._new_route(route)

    def _new_route(self, route: Route):

        global Routes
        global DesiredContainers
        global SdkObject

        newRoute = Route(route.prefix, route.nh_set)

        Routes.add(newRoute)

        dc = [dc for dc in DesiredContainers if dc.nh_set == newRoute.nh_set]
        
        if dc:
            newRoute.dc = dc
            dc.ref_count += 1
        else:
            dc = DesiredContainer()
            newRoute.dc = dc
            dc.ref_count = 1
            dc.nh_set = newRoute.nh_set
            self._allocate_new_ac(dc)
            DesiredContainers.add(dc)
        if dc.State != DesiredContainer.State.FAILED:
            SdkObject.SDKProgramRoute(newRoute)

        self._periodic()


    def _change_route(self, route: Route):
        pass

    def del_route(self, route):
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

    print(ch)
    print(Routes)
    print("\n".join(str(dc) for dc in DesiredContainers))
    print(ActualContainers)


