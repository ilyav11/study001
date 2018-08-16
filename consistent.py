import ipaddress
import enum
import copy


class pSet:
    def __init__(self, s):
        self._s = copy.copy(s)
    
    @property
    def s(self):
        return self._s

    @s.setter
    def s(self, s):
        self._s = copy.copy(s)

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

    @property
    def prefix(self):
        return self._prefix

    @prefix.setter
    def prefix(self, p: Prefix):
        self._prefix = copy.copy(p)

    @property
    def nh_set(self):
        return self._nhset

    @nh_set.setter
    def nh_set(self, nh_set):
        self._nhset = copy.copy(nh_set)
    
    @property
    def desired_container(self):
        return self._dc

    @desired_container.setter
    def desired_container(self, dc):
        self._dc = dc

    def __str__(self):
        return str(self._prefix) + "\t-->\t{" + ", ".join(str(s) for s in self._nhset) + "}"  + "(0x{:02X})".format(id(self._dc))

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
        return "\n".join(str(self._d[s]) for s in self._d.keys())

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

    @resolved.setter
    def resolved(self, res):
        self.resolved = res

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
        self._nh_set = set()
        self._ac = None
        self._child_set = set()
        self._father = None
        self._ref_count = 0

    def __del__(self):
        self._current_state = self.State.FAILED
        self._nh_set = None
        self._ac = None
        self._child_set = None
        self._father = None
        self._ref_count = 0
      
   
    @property
    def nh_set(self):
        return self._nh_set.s

    @nh_set.setter
    def nh_set(self, nh_set):
        self._nh_set = pSet(nh_set)

    @property
    def actual_container(self):
        return self._ac

    @property
    def child_set(self):
        return self._child_set

    @property
    def father(self):
        return self._father

    @father.setter
    def father(self, f):
        self._father = f

    @property
    def ref_count(self):
        return self._ref_count

    @property
    def current_state(self):
        return self._current_state

    @current_state.setter
    def current_state(self, newS):
        self._current_state = newS

    @ref_count.setter
    def ref_count(self, ref_c: int):
        self._ref_count = ref_c

    def __str__(self):
        return "id: " + "0x{:02X}".format(id(self)) + "\n" + "\n".join(str(a) + ": " + str(b) for a,b in vars(self).items())

DesiredContainers = pSet(set())

class SDK:
    def __init__(self):
        pass

    def SDKProgramRoute(self, route: Route):
        pass

    def SDKCloneAC(self, ac: ActualContainer):
        return ac

    def SDKAlign(self, ac: ActualContainer, nhset):
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
        self._system_stable = self.SystemState.STABLE #need to be stable
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
            newRoute.desired_container = dc
            dc.ref_count += 1
        else:
            dc = DesiredContainer()
            newRoute.desired_container = dc
            dc.ref_count = 1
            dc.nh_set = copy.copy(newRoute.nh_set)
            self._allocate_new_ac(dc)
            DesiredContainers.s.add(dc)
        if dc.State != DesiredContainer.State.FAILED:
            SdkObject.SDKProgramRoute(newRoute)

        self._periodic()


    def _change_route(self, newR: Route):

        currR: Route
        currDC: DesiredContainer

        currR = Routes[newR.prefix]

        currDC = currR.desired_container
        currDC.ref_count -= 1

        if currDC.ref_count == 0:
            DesiredContainers.s.remove(currDC)

        currR.nh_set = newR.nh_set

        if self._system_stable == self.SystemState.STABLE:
            dc_child_list = [dc for dc in currDC.child_set if dc.nh_set == newR.nh_set] # TODO how I compare nh_set?
            if len(dc_child_list) > 1:
                raise AssertionError
            if len(dc_child_list) != 0:
                dc: DesiredContainer

                dc = dc_child_list[0]
                currR.desired_container = dc
                dc.ref_count += 1
            else:
                ac: ActualContainer

                dc = DesiredContainer()
                DesiredContainers.s.add(dc)
                currR.desired_container = dc
                dc.nh_set = newR.nh_set
                dc.ref_count = 1
                currDC.child_set.add(dc)
                dc.father = currDC
                ac = SdkObject.SDKCloneAC(currDC.actual_container)
                if ac != None:
                    SdkObject.SDKAlign(ac, dc.nh_set)
                    ac.resolved = True
                    dc.current_state = DesiredContainer.State.RESOLVED
                    dc.actual_container = ac
                else:
                    self._system_consistent = self.SystemState.NON_STABLE
                    self._allocate_new_ac(dc)
                    self._clean_stable_state()
                    self._optimize_non_stable()
        else:

            dc_list = [dc for dc in DesiredContainers if dc.nh_set == newR.nh_set]
            if len(dc_list) > 1:
                raise AssertionError
            if len(dc_list) == 1:
                dc = dc_list[0]
                dc.ref_count += 1
                currR.desired_container = dc
            else:
                dc = DesiredContainer()
                currR.desired_container = dc
                dc.ref_count = 1
                dc.nh_set = newR.nh_set
                self._allocate_new_ac(dc)
                DesiredContainers.s.add(dc)

        if currR.desired_container.current_state != DesiredContainer.State.FAILED:
            SdkObject.SDKProgramRoute(currR)

        self._periodic()

        
    def del_route(self, route: Route):
        pass


    def _periodic(self):
        pass

    def periodic_tick(self):
        global _current_time

        _current_time += self._periodic_timer

    def _allocate_new_ac(self, dc: DesiredContainer):
        pass

    def _clean_stable_state(self):
        pass

    def _optimize_non_stable(self):
        pass

    def __str__(self):
        return str(vars(self))




if __name__ == "__main__":
    pass


