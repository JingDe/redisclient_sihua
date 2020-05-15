#include "inet_addr.h"
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <assert.h>
#include <stdint.h>
#include "base_library/log.h"
#include <sstream>

namespace GBDownLinker {

InetAddr::InetAddr() {
	this->addrType = AF_INET;
}

InetAddr::~InetAddr() {

}


void InetAddr::setInternAddressV4(struct in_addr __address) {
	address.sin_addr = __address;
}

void InetAddr::setInternAddressV6(struct in6_addr __address) {
	address.sin6_addr = __address;
}

void InetAddr::setAddrType(int addrType) {
	this->addrType = addrType;
}

/** Determines the IP address of a host, given the host's name.
 */
InetAddr InetAddr::getByName(const string& host) 
{
	InetAddr address;
	struct addrinfo* result = NULL;
	getaddrinfo(host.c_str(), 0, 0, &result);
	if (result == NULL) {
		std::stringstream log_msg;
		log_msg << "Cannot get information about host " << host << ", " << errno;
		LOG_WRITE_ERROR(log_msg.str());
		throw std::runtime_error("InetAddr Unknow host");
	}
	
	if (result->ai_family == AF_INET) {
		struct sockaddr_in* addr4 = (struct sockaddr_in*)result->ai_addr;
		address.address.sin_addr = addr4->sin_addr;
		address.addrType = AF_INET;
	}
	else if (result->ai_family == AF_INET6) {
		struct sockaddr_in6* addr6 = (struct sockaddr_in6*)result->ai_addr;
		address.address.sin6_addr = addr6->sin6_addr;
		address.addrType = AF_INET6;
	}
	freeaddrinfo(result);

	return address;
}

string InetAddr::getHostAddress() const {
	if (this->addrType == AF_INET) {
		struct in_addr addr = address.sin_addr;
		return string(::inet_ntoa(addr));
	}
	else if (this->addrType == AF_INET6) {
		char addr_str[2048];
		::inet_ntop(AF_INET6, &(address.sin6_addr), addr_str, 2048);
		return string(addr_str);
	}
	else {
		return "";
	}
}

/** Gets the host name for this IP address.
 */
string InetAddr::getHostName() const {
	char hostname[1024];
	struct sockaddr* addr;

	if (this->addrType == AF_INET) {
		struct sockaddr_in addr4;
		addr4.sin_addr = address.sin_addr;
		addr4.sin_family = AF_INET;
		addr = (struct sockaddr*) & addr4;
		::getnameinfo(addr, sizeof(struct sockaddr_in), hostname, 1024, 0, 0, 0);
	}
	else if (this->addrType == AF_INET6) {
		struct sockaddr_in6 addr6;
		addr6.sin6_addr = address.sin6_addr;
		addr6.sin6_family = AF_INET6;
		addr = (struct sockaddr*) & addr6;
		::getnameinfo(addr, sizeof(struct sockaddr_in6), hostname, 1024, 0, 0, 0);
	}
	else {

	}

	if (strlen(hostname) == 0) {
		return "";
	}
	else {
		return string(hostname);
	}
}

/** Returns the local host.
 */
InetAddr InetAddr::getLocalHost() {
	return getLoopAddrV4();
}

InetAddr  InetAddr::getLoopAddrV4() {
	InetAddr address;
	address.address.sin_addr.s_addr = inet_addr("127.0.0.1");
	address.addrType = AF_INET;
	return address;
}
InetAddr  InetAddr::getLoopAddrV6() {
	InetAddr address;
	struct in6_addr addr;
	inet_pton(AF_INET6, "::1", &addr.s6_addr);
	address.addrType = AF_INET6;
	return address;
}

/** Utility routine to check if the InetAddr is an IP multicast address.
 IP multicast address is a Class D address
 i.e first four bits of the address are 1110.
 */
bool InetAddr::isMulticastAddress() const {
	if (addrType == AF_INET) {
		uint32_t __address;
		memcpy(&__address, &(address.sin_addr.s_addr), sizeof(uint32_t));
		return (__address & 0xF000) == 0xE000;
	}
	else if (addrType == AF_INET6) {
		if (IN6_IS_ADDR_MULTICAST(&(address.sin6_addr))) {
			return true;
		}
		else {
			return false;
		}
	}
	else {
		std::stringstream log_msg;
		log_msg << "invalid net addr !";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
}

/** Converts this IP address to a String.
 */
string InetAddr::toString() const {
	return getHostName() + "/" + getHostAddress();
}

int InetAddr::getAddrType() const {
	return this->addrType;
}

} // namespace GBDownLinker
