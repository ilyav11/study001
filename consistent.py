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
        self._nhset = set()
        self._nhset = nhset


    def set_prefix(self, prefix):
        self._prefix = prefix

    def set_nhset(self, nhset):
        self._nhset = nhset

    @property
    def prefix(self):
        return self._prefix

    @property
    def NHSet(self):
        return self._nhset

    def __str__(self):
        return str(self._prefix) + "-->{" + ", ".join(str(s) for s in self._nhset) + "}"

Routes = set()


class ActualContainer:
    def __init__(self):
        pass


ActualContainers = set()


class DesiredContainer:
    def __init__(self):
        pass

DesignatedContainers = set()

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





class ConsistentHash:

    class SystemState(enum.Enum):
        STABLE = 1
        NON_STABLE = 2

    class SystemConsistent(enum.Enum):
        CONSISTENT = 1
        NON_CONSISTENT = 2

    
    def __init__(self):
        pass

    
    def add_route(self, route):
        pass

    def del_route(self, route):
        pass



p = Prefix()
p.set_prefix("192.168.1.1/24")

nh1 = Nexthop("10.0.0.1")
nh2 = Nexthop("10.0.0.2")
s = set([nh1, nh2])

r = Route(p, s)

print(r)

