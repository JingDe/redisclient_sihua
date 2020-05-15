#ifndef DDR_INET_ADDR_H_
#define DDR_INET_ADDR_H_


#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string>
#include <exception>
#include <stdexcept>

using std::string;

namespace GBDownLinker {

class InetAddr
{
public:
	InetAddr();
	virtual ~InetAddr();

	void setInternAddressV4(struct in_addr __address);
	void setInternAddressV6(struct in6_addr __address);

	/** Determines all the IP addresses of a host, given the host's name.
	*/
	//    static std::vector<InetAddr> getAllByName(const string& host);

		/** Determines the IP address of a host, given the host's name.
		*/
	static InetAddr getByName(const string& host) ;

	/** Returns the IP address string "%d.%d.%d.%d".
	*/
	string getHostAddress() const;

	/** Gets the host name for this IP address.
	*/
	string getHostName() const;

	/** Returns the local host.
	*/
	static InetAddr  getLocalHost();
	/** Returns the local host.
	*/
	static InetAddr  getLoopAddrV4();
	static InetAddr  getLoopAddrV6();

	/** Utility routine to check if the InetAddr is an IP multicast address.
	*/
	bool isMulticastAddress() const;

	int getAddrType() const;

	void setAddrType(int adrType);


	/** Converts this IP address to a string.
	*/
	string toString() const;


		//static OWMutexLock m_lock;
	union {
		struct in_addr  sin_addr;     //IPv4
		struct in6_addr sin6_addr; //IPv6
	} address;

private:
	string hostname;
	int addrType;;
};

} // namespace GBDownLinker

#endif /*INET_ADDR_H_*/
