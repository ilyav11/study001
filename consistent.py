import ipaddress
import enum

import copy
import logging
import random

import sched
import threading


_TRACE_LEVEL = 1

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

    def __len__(self):
        return len(self._s)

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

    def delete(self):
        self._log.log(_TRACE_LEVEL, "deleting a_cont %s\n", str(self))
        pass


    @property
    def desired_container(self):
        return self._dc

    @desired_container.setter
    def desired_container(self, dc):
        self._dc = dc

    @property
    def resolved(self):
        return self._resolved

    @resolved.setter
    def resolved(self, res):
        self._resolved = res

    @property
    def consistent(self):
        return self._consistent

    @consistent.setter
    def consistent(self, val: bool):
        self._consistent = val

    @property
    def nh_set(self):
        return self._nh_set

    @nh_set.setter
    def nh_set(self, nhset):
        self._nh_set = pSet(nhset)

    def __str__(self):

        s = "id: 0x{:X} ".format(id(self))
        
        for a,b in vars(self).items():
            if a == "_dc":
                s += " {} : 0x{:X}".format(a,id(b))
            else:
                s += " {} : {}".format(a,b)
        return s + "\n"
        

class DesiredContainer:

    class State(enum.Enum):
        RESOLVED = 1
        FAILED = 2
        PARTIAL = 3
        REALLOCATE = 4

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

        self._log.log(_TRACE_LEVEL, "deleting d_cont %s\n", str(self))

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


        if self._ac:
            self._ac.delete()
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

    @actual_container.setter
    def actual_container(self, ac: ActualContainer):
        self._ac = ac

    @property
    def child_set(self):
        return self._child_set

    @child_set.setter
    def child_set(self, ch_set):
        self._child_set = ch_set

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
            elif a == "_ac":
                if b == None:
                    s += ", {}: None".format(a)
                else:
                    s += ", {}: 0x{:02X}".format(a,id(b))                
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

    def __init__(self, log: logging.Logger, memory = 7):
        self._log = log.getChild("sdk")
        self._memory = memory
        
        

    def SDKProgramRoute(self, route: Route):

        self._log.log(_TRACE_LEVEL, "route=%s memory=%d", str(route), self._memory)
        assert route.desired_container.actual_container != None

    def SDKCloneAC(self, ac: ActualContainer):
        self._log.log(_TRACE_LEVEL, "ac=%s memory=%d", str(ac), self._memory)
        
        new_ac = self.SDKCreateContainer(ac.nh_set, ac.consistent)
        return new_ac

    def SDKAlign(self, ac: ActualContainer, nhset):
        self._log.log(_TRACE_LEVEL, "ac = %s nhset=%s memory=%d", str(ac), str(pSet(nhset)),  self._memory)
        ac.nh_set = nhset


    def SDKCreateContainer(self, nhset, if_consistent: bool):

        self._log.log(_TRACE_LEVEL, "nhset=%s, consistent=%s memory=%d", str(pSet(nhset)), str(if_consistent), self._memory)

        if if_consistent:
            total = SDK._CONSISTENT_HASH_SIZE
        else:
            total = len(nhset)* SDK._SINGLE_HASH_SIZE
        
        if self._memory >= total:
            self._memory -= total
            self._log.log(logging.DEBUG, "Created container nhset=%s consistent=%s size=%d(memory=%d)", str(pSet(nhset)), str(if_consistent), total, self._memory)
        else:
            self._log.log(logging.DEBUG, "Failed to allocate container nh=%s consistent=%s %d(memory=%d)", str(pSet(nhset)), str(if_consistent), total, self._memory)
            return None

        ac = ActualContainer(self._log)

        ac.consistent = if_consistent

        return ac


    def SDKDeleteContainer(self, ac: ActualContainer):
        
        self._log.log(_TRACE_LEVEL, "delete: ac=%s, memory=%d", str(ac), self._memory)

        free_mem = 0
        if ac.consistent:
            free_mem = SDK._CONSISTENT_HASH_SIZE
        else:
            free_mem = len(ac.nh_set) * SDK._SINGLE_HASH_SIZE

        self._memory += free_mem

        self._log.log(logging.DEBUG, "Deleted container nhset=%s consistent=%s size=%d(memory=%d)", str(ac.nh_set), str(ac.consistent), free_mem, self._memory)

            
    def SDKReplaceContainer(self, ac1: ActualContainer, ac2: ActualContainer):
        self._log.log(_TRACE_LEVEL, "ac1= [%s] ac2=  [%s] memory=%d", str(ac1), str(ac2), self._memory)

    def __str__(self):
        return "".join(" {}: {}".format(a,b) for a,b in vars(self).items())


class ConsistentHash:

    class SystemState(enum.Enum):
        STABLE = 1
        NON_STABLE = 2

    class SystemResolved(enum.Enum):
        RESOLVED = 1
        NOT_RESOLVED = 2

    _long_period_of_time = 4 #seconds

    _periodic_timer = 2 #seconds

    _timer_tick = 1 #sec

    _degrage_max = 10
    
    def __init__(self, debug_level = _TRACE_LEVEL):
        self._log = logging.getLogger("c_hash")
        self.DesiredContainers = cDesiredContainers(self._log)
        self.SdkObject = SDK(self._log)
        self.ActualContainers = pSet(set())
        self.Routes = RouteContainer(self._log)

        self._system_resolved = self.SystemResolved.RESOLVED
        self._system_stable = self.SystemState.STABLE #need to be stable
        self._last_resolved = 0
        self._consistent_adm = False

        self._sched = sched.scheduler()
        self._current_time = 0
        self._last_periodic = 0
        self._lock = threading.Lock()

        self._running = True
        self._freeze = False

        FORMAT = '%(asctime)-15s %(levelname)-8s [%(filename)s:%(lineno)d - %(funcName)20s()] %(message)s'

        logging.basicConfig(filename = "log.txt", format=FORMAT, level= debug_level, filemode = "w")

        #start ticking
        self._sched.enter(1, 1, self._periodic_tick)
        self._timer_thread = threading.Thread(target=self._sched.run)
        

    
    def add_route(self, route: Route):

        self._lock.acquire()

        if route.prefix in self.Routes.prefixes():
            self._change_route(route)
        else:
            self._new_route(route)
        
        self._lock.release()

    def _new_route(self, route: Route):

        newRoute = Route(route.prefix, route.nh_set)

        self._log.log(_TRACE_LEVEL, "New route %s\n", str(newRoute))

        self.Routes.add(newRoute)

        l_dc = [dc for dc in self.DesiredContainers if dc.nh_set == newRoute.nh_set]
        
        if l_dc:
            if len(l_dc)!= 1:
                raise AssertionError
            dc = l_dc[0]
            newRoute.desired_container = dc
            dc.ref_count += 1

            self._log.log(_TRACE_LEVEL, "Adding route %s to existing d_cont %s\n", str(newRoute), str(dc))

            return

        dc: DesiredContainer

        dc = DesiredContainer(self._log)
        newRoute.desired_container = dc
        dc.ref_count = 1
        dc.nh_set = copy.copy(newRoute.nh_set)
        self._allocate_new_ac(dc)
        self.DesiredContainers.add(dc)

        self._log.log(_TRACE_LEVEL, "Creating container %s for route %s\n", str(dc), str(newRoute))

        if dc.current_state != DesiredContainer.State.FAILED:
            self.SdkObject.SDKProgramRoute(newRoute)

        if dc.current_state != DesiredContainer.State.RESOLVED:
            self._periodic(lock = False) #in case if desired container was failed - need to check current state


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
                self._log.log(_TRACE_LEVEL, "Creating container %s for route %s\n", str(dc), str(currR))
            else:
                ac: ActualContainer

                dc = DesiredContainer(self._log)
                self.DesiredContainers.add(dc)
                currR.desired_container = dc
                dc.nh_set = newR.nh_set
                dc.ref_count = 1
                currDC.child_set.add(dc)
                dc.father = currDC

                self._log.log(_TRACE_LEVEL, "Creating container %s for route %s\n", str(dc), str(currR))

                assert currDC.actual_container

                ac = self.SdkObject.SDKCloneAC(currDC.actual_container)
                if ac:
                    ac.nh_set = currDC.actual_container.nh_set
                    self.SdkObject.SDKAlign(ac, dc.nh_set)
                    ac.resolved = True
                    dc.current_state = DesiredContainer.State.RESOLVED
                    dc.actual_container = ac
                    
                    ac.desired_container = dc
                    self.ActualContainers.s.add(ac)
                else:
                    self._system_stable = self.SystemState.NON_STABLE
                    self._system_resolved = self.SystemResolved.NOT_RESOLVED
                    self._allocate_new_ac(dc)
                    self._clean_stable_state()
                    self._optimize_non_stable()
        else:

            dc_list = [dc for dc in self.DesiredContainers if dc.nh_set == newR.nh_set]
            if len(dc_list) > 1:
                raise AssertionError
            if len(dc_list) == 1:
                dc = dc_list[0]
                dc.ref_count += 1
                currR.desired_container = dc
            else:
                dc = DesiredContainer(self._log)
                currR.desired_container = dc
                dc.ref_count = 1
                dc.nh_set = newR.nh_set
                self._allocate_new_ac(dc)
                self.DesiredContainers.add(dc)
                self._log.log(_TRACE_LEVEL, "Creating container %s for route %s\n", str(dc), str(currR))

        if currDC.ref_count == 0:
            self.DesiredContainers.remove(currDC)
            
            if currDC.actual_container != None:
                self.ActualContainers.s.remove(currDC.actual_container)
                self.SdkObject.SDKDeleteContainer(currDC.actual_container)
            currDC.delete()

        if currR.desired_container.current_state != DesiredContainer.State.FAILED:
            self.SdkObject.SDKProgramRoute(currR)

        self._periodic(lock = False)

        
    def del_route(self, route: Route):

        self._lock.acquire()
        
        currR: Route
        currDC: DesiredContainer

        self._log.log(_TRACE_LEVEL, "route=%s", str(route))

        if route.prefix in self.Routes.prefixes():
            

            currR = self.Routes[route.prefix]
            currDC = currR.desired_container

            currDC.ref_count -= 1

            if currDC.ref_count == 0:
                self.DesiredContainers.remove(currDC)

                if currDC.actual_container != None:
                    self.ActualContainers.s.remove(currDC.actual_container)
                    self.SdkObject.SDKDeleteContainer(currDC.actual_container)
    
                currDC.delete()
                self._periodic(lock = False)
            
            self.Routes.remove(currR)
        
        self._lock.release()

    def _periodic(self, lock = True):

        if lock:
            self._lock.acquire()

        self._log.log(_TRACE_LEVEL, "Periodic: timer=%d", self._current_time)

        if self._system_resolved != self.SystemResolved.RESOLVED:
            self._optimize_not_resolved()
            self._check_for_resolution()
        if self._system_stable != self.SystemState.STABLE:
            self._check_for_stable()

        if lock:
            self._lock.release()


    def _periodic_tick(self):
        
        if self._running:
            self._sched.enter(1, 1, self._periodic_tick)

            self._current_time += self._timer_tick

            if self._freeze == False:
                if self._current_time - self._last_periodic  >= self._periodic_timer:
                    self._last_periodic = self._current_time
                    self._periodic()
        

    def run(self):
        self._running = True
        self._timer_thread.start()
   

    def stop(self):
        self._running = False


    def freeze(self):
        self._freeze = True

    def unfreeze(self):
        self._freeze = False

    def _allocate_new_ac(self, dc: DesiredContainer):
        ac: ActualContainer

        self._log.log(_TRACE_LEVEL, "dc= %s", str(dc))

        ac = self._create_new_ac(dc.nh_set, True)

        if ac and ac.resolved:
            dc.current_state = DesiredContainer.State.RESOLVED
            dc.actual_container = ac
            self.ActualContainers.s.add(ac)

            ac.desired_container = dc

            self._log.log(_TRACE_LEVEL, "creating new ac: ac= %s", str(ac))

            return ac
        else:
            self._system_resolved = False
            if self._system_stable == self.SystemState.STABLE:
                self._system_stable = self.SystemState.NON_STABLE
                self._clean_stable_state()
                self._optimize_non_stable()
                self._last_resolved = self._now()
                self._log.log(_TRACE_LEVEL, "system state change: %s", str(self))

            if ac:

                dc.actual_container = ac

                if ac.resolved:
                    dc.current_state = dc.State.RESOLVED
                else:
                    dc.current_state = dc.State.PARTIAL
                self.ActualContainers.s.add(ac)

                ac.desired_container = dc

                self._log.log(_TRACE_LEVEL, "creating new ac: ac= %s", str(ac))

                return ac

        dc.current_state = dc.State.FAILED

        self._log.log(_TRACE_LEVEL, "AC alloction failed for dc=%s", str(dc))

        return None

#         AC.DC = DC
#         ACs += AC
#         DC.State = AC.Resolved ? Resolved : Partial
#         DC.AC = AC
#         return AC
#     else:
#         DC.State = Failed
#         return None
                      
    def _create_new_ac(self, nhset, fallback: bool, force_partial = False):
        ac: ActualContainer

        self._log.log(_TRACE_LEVEL, "nhset=%s fallback=%s force_partial=%s", str(pSet(nhset)), str(fallback), str(force_partial))


        if force_partial == False:
            ac = self.SdkObject.SDKCreateContainer(nhset, self._consistent_adm)
            if ac:
                ac.nh_set = nhset
                ac.resolved = True
                return ac
        
        if fallback:
            any_nh_id = random.randint(0, len(nhset)-1)
            any_nh = pSet(set())
            any_nh.s.add(list(nhset)[any_nh_id])

            ac = self.SdkObject.SDKCreateContainer(any_nh, False)
            if ac:
                ac.nh_set = any_nh
                ac.resolved = False
                return ac
        
        return None


    def _clean_stable_state(self):

        dc: DesiredContainer

        for dc in self.DesiredContainers:
            dc.child_set = set()
            dc.father = None

    def _optimize_non_stable(self):

        for dc in self.DesiredContainers:
            equal = [dc1 for dc1 in self.DesiredContainers if dc1.nh_set == dc.nh_set]
            programmed = [dc1 for dc1 in equal if dc1.current_state != DesiredContainer.State.FAILED]

            if len(equal) == 1:
                continue
            
            if len(programmed) == 0:
                return

            c_dc = programmed[0]

            equal = equal.pop(c_dc)

            for dc1 in equal:
                if dc1.current_state != DesiredContainer.State.FAILED:
                    self.SdkObject.SDKReplaceContainer(dc1.AC, c_dc.AC)
                routes = [route for route in self.Routes if route.desired_container == dc1]
                for route in routes:
                    route.desired_container = dc
                    dc.ref_count += 1

                if dc1.actual_container != None:
                    self.ActualContainers.s.remove(dc1.actual_container)
                    self.SdkObject.SDKDeleteContainer(dc1.actual_container)
                    dc1.actual_container.delete()
                 
                dc1.delete()

            
    def _optimize_not_resolved(self):

        dc: DesiredContainer

        self._log.log(_TRACE_LEVEL,"enter")

        for dc in self.DesiredContainers:
            if dc.current_state == DesiredContainer.State.RESOLVED:
                continue
            old_ac = dc.actual_container
            ac = self._create_new_ac(dc.nh_set, fallback = False)
            if ac:

                self.SdkObject.SDKReplaceContainer(dc.actual_container, ac)
                dc.actual_container = ac
                dc.current_state = DesiredContainer.State.RESOLVED

                self.ActualContainers.s.add(ac)

                #TODO add the check
                if old_ac:
                    self.ActualContainers.s.remove(old_ac)
                    self.SdkObject.SDKDeleteContainer(old_ac)
                    old_ac.delete()

    def _check_for_resolution(self):
        if self._system_resolved != self.SystemResolved.RESOLVED:
            non_resolved = [dc for dc in self.DesiredContainers if dc.current_state != DesiredContainer.State.RESOLVED]

            if len(non_resolved) == 0:
                self._system_resolved = self.SystemResolved.RESOLVED
                self._last_resolved = self._now()  
                self._log.log(_TRACE_LEVEL, "State resolved time=%d", self._current_time)      


    def _check_for_stable(self):

        self._log.log(_TRACE_LEVEL, "%s", str(self))
        if self._system_stable != self.SystemState.STABLE:
            if (self._system_resolved == self.SystemResolved.RESOLVED) and \
                ((self._now() - self._last_resolved >= self._long_period_of_time) or self._consistent_adm == False):

                self._system_stable = self.SystemState.STABLE
                self._log.log(_TRACE_LEVEL, "State stable time=%d", self._current_time)
    
    def set_admin_state(self, consistent_adm):
        if self._consistent_adm == consistent_adm:
            return
        
        self._consistent_adm = consistent_adm

        self._system_resolved = self.SystemResolved.NOT_RESOLVED
        self._system_stable = self.SystemState.NON_STABLE

        for dc in self.DesiredContainers:
            dc.current_state = DesiredContainer.State.REALLOCATE


    def __str__(self):
        return str(vars(self))

    def _now(self):
        return self._current_time



if __name__ == "__main__":
    pass


