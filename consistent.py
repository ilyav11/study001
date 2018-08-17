import ipaddress
import enum
import copy
import logging
import random


class pSet:
    def __init__(self, s):
        self._s = copy.copy(s)
    
    @property
    def s(self):
        return self._s

    @s.setter
    def s(self, s):
        self._s = copy.copy(s)

    @s.deleter
    def s(self):
        self._s = set()

    def __str__(self):
        return "{" +  ", ".join(str(s) for s in self._s) + "}"

    def __iter__(self):
        for s in self._s:
            yield s

    def __eq__(self, other):
        return self._s == other._s

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

    def __eq__(self, other):
        return self._ipaddr  == other._ipaddr

    def __hash__(self):
        return hash(self._ipaddr)


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

    @prefix.deleter
    def prefix(self):
        self._prefix = Prefix()

    @property
    def nh_set(self):
        return self._nhset

    @nh_set.setter
    def nh_set(self, nh_set):
        self._nhset = copy.copy(nh_set)
    
    @nh_set.deleter
    def nh_set(self):
        self._nhset = set()
    
    @property
    def desired_container(self):
        return self._dc

    @desired_container.setter
    def desired_container(self, dc):
        self._dc = dc

    @desired_container.deleter
    def desired_container(self):
        self._dc = None

    def __str__(self):
        return str(self._prefix) + "\t-->\t{" + ", ".join(str(s) for s in self._nhset) + "}"  + "(0x{:02X})".format(id(self._dc))

class RouteContainer:
    def __init__(self, log: logging.Logger):
        self._d = {}
        self._log = log.getChild("r_cont")
    
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

class ActualContainer:
    def __init__(self, log: logging.Logger):
        self._dc = None
        self._resolved = False
        self._nh_set = set()
        self._log = log.getChild("a_cont")
        self._consistent = False

    @property
    def desired_container(self):
        return self._dc

    @property
    def resolved(self):
        return self._resolved

    @resolved.setter
    def resolved(self, res):
        self.resolved = res

    @resolved.deleter
    def resolved(self):
        self.resolved = False

    @property
    def consistent(self):
        return self._consistent

    @consistent.setter
    def consistent(self, val: bool):
        self._consistent = val

    @property
    def nh_set(self):
        return self._nh_set


class DesiredContainer:

    class State(enum.Enum):
        RESOLVED = 1
        FAILED = 2
        PARTIAL = 3

    def __init__(self, log: logging.Logger):
        self._current_state = self.State.FAILED
        self._nh_set = set()
        self._ac = None
        self._child_set = set()
        self._father = None
        self._ref_count = 0
        self._log = log.getChild("d_cont")

    def delete(self):

        # need to delete all references to father

        self._log.debug("deleting container %s\n", str(self))

        for dc in self._child_set:
            dc.father =  None

        #remove child list
        self._child_set = None

        # need to update father

        if self._father:
            self._father.child_set.remove(self)

        #delete father

        self._father = None

        self._current_state = self.State.FAILED
        self._nh_set = None
        self._ac = None
        self._ref_count = 0
      
   
    @property
    def nh_set(self):
        return self._nh_set.s

    @nh_set.setter
    def nh_set(self, nh_set):
        self._nh_set = pSet(nh_set)

    @nh_set.deleter
    def nh_set(self):
        self._nh_set = pSet(set())

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

    @father.deleter
    def father(self):
        self._father = None

    @property
    def ref_count(self):
        return self._ref_count

    @property
    def current_state(self):
        return self._current_state

    @current_state.setter
    def current_state(self, newS):
        self._current_state = newS

    @current_state.deleter
    def current_state(self):
        self._current_state =  self.State.FAILED

    @ref_count.setter
    def ref_count(self, ref_c):
        self._ref_count = ref_c

    @ref_count.deleter
    def ref_count(self):
        self._ref_count = 0

    def print_me(self, level):
        
        s =  "id " +  "0x{:02X}".format(id(self))

        for a,b in vars(self).items():
            if a == "_father":
                if self._father == None:
                    s += ", _father: None"
                else:
                    s += ", _father: 0x{:02X}".format(id(self._father))
            else:
                s += ", {} : {}".format(a,b)

        if self._child_set:
                for c in self._child_set:
                    s += "\n" + "\t"*(level+1)
                    s += c.print_me(level + 1)
        return s

    def __str__(self):
        s =  "id " +  "0x{:02X}".format(id(self))

        for a,b in vars(self).items():
            if a == "_father":
                if self._father == None:
                    s += ", _father: None"
                else:
                    s += ", _father: 0x{:02X}".format(id(self._father))
            else:
                s += ", {} : {}".format(a, b)
        return s

class cDesiredContainers:
    def __init__(self, log: logging.Logger):
        self._log = log.getChild("d_conts")
        self._s = set()

    def __str__(self):

        st = ""
        for s in self._s:
            if s.father == None:
                st += s.print_me(0) + "\n"
        return st

    def __iter__(self):
        for s in self._s:
            yield s

    def add(self, dc: DesiredContainer):
        self._s.add(dc)

    def remove(self, dc: DesiredContainer):
        self._s.remove(dc)


    

class SDK:

    _CONSISTENT_HASH_SIZE = 5
    _SINGLE_HASH_SIZE = 1

    def __init__(self, log: logging.Logger, memory = 20):
        self._log = log.getChild("sdk")
        self._memory = memory
        
        

    def SDKProgramRoute(self, route: Route):
        pass

    def SDKCloneAC(self, ac: ActualContainer):
        new_ac = self.SDKCreateContainer(ac.nh_set, ac.consistent)

        return new_ac



    def SDKAlign(self, ac: ActualContainer, nhset):
        pass

    def SDKCreateContainer(self, nhset, if_consistent: bool):
        if if_consistent:
            total = len(nhset)* SDK._CONSISTENT_HASH_SIZE
        else:
            total = len(nhset)* SDK._SINGLE_HASH_SIZE
        
        if self._memory >= total:
            self._memory -= total
        else:
            return None

        ac = ActualContainer(self._log)

        ac.consistent = if_consistent

        return ac


    def SDKDeleteContainer(self, ac: ActualContainer):
        if ac.consistent:
            self._memory += len(ac.nh_set) * SDK._CONSISTENT_HASH_SIZE
        else:
            self._memory += len(ac.nh_set) * SDK._SINGLE_HASH_SIZE


class ConsistentHash:

    class SystemState(enum.Enum):
        STABLE = 1
        NON_STABLE = 2

    class SystemResolved(enum.Enum):
        RESOLVED = 1
        NOT_RESOLVED = 2

    _long_period_of_time = 600 #seconds

    _periodic_timer = 30 #seconds
    
    def __init__(self):
        self._log = logging.getLogger("c_hash")
        self.DesiredContainers = cDesiredContainers(self._log)
        self.SdkObject = SDK(self._log)
        self.ActualContainers = pSet(set())
        self.Routes = RouteContainer(self._log)

        self._system_resolved = self.SystemResolved.RESOLVED
        self._system_stable = self.SystemState.STABLE #need to be stable
        self._last_resolved = 0
        self._consistent_adm = False

        FORMAT = '%(asctime)-15s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'

        logging.basicConfig(filename = "log.txt", format=FORMAT, level=logging.DEBUG, filemode = "w")

    
    def add_route(self, route: Route):
        if route.prefix in self.Routes.prefixes():
            self._change_route(route)
        else:
            self._new_route(route)

    def _new_route(self, route: Route):

        newRoute = Route(route.prefix, route.nh_set)

        self.Routes.add(newRoute)

        l_dc = [dc for dc in self.DesiredContainers if dc.nh_set == newRoute.nh_set]
        
        if l_dc:
            if len(l_dc)!= 1:
                raise AssertionError
            dc = l_dc[0]
            newRoute.desired_container = dc
            dc.ref_count += 1
        else:
            dc = DesiredContainer(self._log)
            newRoute.desired_container = dc
            dc.ref_count = 1
            dc.nh_set = copy.copy(newRoute.nh_set)
            self._allocate_new_ac(dc)
            self.DesiredContainers.add(dc)

            self._log.debug("Creating container %s for route %s\n", str(dc), str(newRoute))

        if dc.State != DesiredContainer.State.FAILED:
            self.SdkObject.SDKProgramRoute(newRoute)

        self._periodic()


    def _change_route(self, newR: Route):

        currR: Route
        currDC: DesiredContainer

        currR = self.Routes[newR.prefix]

        currDC = currR.desired_container
        currDC.ref_count -= 1

        currR.nh_set = newR.nh_set

        if (self._system_stable == self.SystemState.STABLE) and (self._consistent_adm == True):
            dc_child_list = [dc for dc in currDC.child_set if dc.nh_set == newR.nh_set] # TODO how I compare nh_set?
            if len(dc_child_list) > 1:
                raise AssertionError
            if len(dc_child_list) != 0:
                dc: DesiredContainer

                dc = dc_child_list[0]
                currR.desired_container = dc
                dc.ref_count += 1
                self._log.debug("Creating container %s for route %s\n", str(dc), str(currR))
            else:
                ac: ActualContainer

                dc = DesiredContainer(self._log)
                self.DesiredContainers.add(dc)
                currR.desired_container = dc
                dc.nh_set = newR.nh_set
                dc.ref_count = 1
                currDC.child_set.add(dc)
                dc.father = currDC

                self._log.debug("Creating container %s for route %s\n", str(dc), str(currR))

                assert currDC.actual_container

                ac = self.SdkObject.SDKCloneAC(currDC.actual_container)
                if ac != None:
                    self.SdkObject.SDKAlign(ac, dc.nh_set)
                    ac.resolved = True
                    dc.current_state = DesiredContainer.State.RESOLVED
                    dc.actual_container = ac
                    self.ActualContainers.s.add(ac)
                else:
                    self._system_stable = self.SystemState.NON_STABLE
                    self._allocate_new_ac(dc)
                    self._clean_stable_state()
                    self._optimize_non_stable()
        else:

            dc_list = [dc for dc in self.DesiredContainers if dc.nh_set == newR.nh_set]
            if len(dc_list) > 1:
                raise AssertionError
            if len(dc_list) == 1:
                dc = dc_list[0]
                print(dc)
                dc.ref_count += 1
                print(dc)
                currR.desired_container = dc
            else:
                dc = DesiredContainer(self._log)
                currR.desired_container = dc
                dc.ref_count = 1
                dc.nh_set = newR.nh_set
                self._allocate_new_ac(dc)
                self.DesiredContainers.add(dc)
                self._log.debug("Creating container %s for route %s\n", str(dc), str(currR))

        if currDC.ref_count == 0:
            self.DesiredContainers.remove(currDC)
            currDC.delete()

        if currR.desired_container.current_state != DesiredContainer.State.FAILED:
            self.SdkObject.SDKProgramRoute(currR)

        self._periodic()

        
    def del_route(self, route: Route):
        
        currR: Route
        currDC: DesiredContainer

        if route.prefix not in self.Routes.prefixes():
            return

        currR = self.Routes[route.prefix]
        currDC = currR.desired_container

        currDC.ref_count -= 1

        if currDC.ref_count == 0:
            self.DesiredContainers.remove(currDC)
            currDC.delete()
            self._periodic()
        
        self.Routes.remove(currR)

    def _periodic(self):
        pass

    def periodic_tick(self):
        pass
        

    def _allocate_new_ac(self, dc: DesiredContainer):
        ac: ActualContainer

        ac = self._create_new_ac(dc.nh_set, False)

        if ac:
            dc.current_state = DesiredContainer.State.RESOLVED
            dc.actual_container = ac
            self.ActualContainers.s.add(ac)

            return ac
        else:
            self._system_resolved = False
            if self._system_stable == self.SystemState.STABLE:
                self._system_stable = self.SystemState.NON_STABLE
                self._clean_stable_state()
                self._optimize_non_stable()
                self._last_resolved = self._now()
            ac = self._create_new_ac(dc.nh_set, True)
            if ac:

                dc.actual_container = ac

                if ac.resolved:
                    dc.current_state = dc.State.RESOLVED
                else:
                    dc.current_state = dc.State.PARTIAL
                self.ActualContainers.s.add(ac)

                return ac

        dc.current_state = dc.State.FAILED

        return None

#         AC.DC = DC
#         ACs += AC
#         DC.State = AC.Resolved ? Resolved : Partial
#         DC.AC = AC
#         return AC
#     else:
#         DC.State = Failed
#         return None
                      
    def _create_new_ac(self, nhset, fallback: bool):
        ac: ActualContainer

        ac = self.SdkObject.SDKCreateContainer(nhset, self._consistent_adm)
        if ac:
            ac.nh_set = nhset
            ac.resolved = True
            return ac
        
        if fallback:
            any_nh_id = random.randint(0, len(nhset))
            any_nh = pSet(set())
            ac = self.SdkObject.SDKCreateContainer(any_nh, False)
            if ac:
                any_nh.s.add(nhset[any_nh_id])
                ac.nh_set = any_nh
                ac.resolved = False
                return ac
        
        return None


    def _clean_stable_state(self):
        pass

    def _optimize_non_stable(self):
        pass

    def __str__(self):
        return str(vars(self))

    def _now(self):
        return 0



if __name__ == "__main__":
    pass


