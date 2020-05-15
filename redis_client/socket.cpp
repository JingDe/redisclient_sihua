#include <assert.h>
#include <sys/types.h>
#include "socket.h"
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/epoll.h>
#include "base_library/log.h"
#include <sstream>

#define gettid() syscall(__NR_gettid)

namespace GBDownLinker {

Socket::Socket() :
	fd(INVALID_SOCKET_HANDLE), m_timeout(-1), localport(-1), port(0)
{
	m_stream = true;
}

Socket::~Socket() {
	close();
}

int Socket::accept(Socket& s) {

	// use poll to set timeout
	if (m_timeout > 0) {
		int retval;
		struct pollfd pfd;
		pfd.fd = fd;
		pfd.events = POLLIN;
		pfd.revents = 0;

		retval = ::poll(&pfd, 1, m_timeout * 1000);  //milliseconds
		if (retval < 0) {
			std::stringstream log_msg;
			log_msg << "poll error, fd " << fd << " at accept: " << strerror(errno);
			LOG_WRITE_ERROR(log_msg.str());
			return -1;
		}
		if (retval == 0) {
			std::stringstream log_msg;
			log_msg << "poll on fd(" << fd << ") at accept return timeout(" << m_timeout << " s)";
			LOG_WRITE_ERROR(log_msg.str());
			return -1;
		}
	}

	if (address.getAddrType() == AF_INET) {
		sockaddr_in client_addr;
		socklen_t client_len;
		client_len = sizeof(client_addr);
		int fdClient = ::accept(this->fd, (sockaddr*)&client_addr, &client_len);
		if (fdClient < 0) {
			std::stringstream log_msg;
			log_msg << "socket accept failed: " << strerror(errno);
			LOG_WRITE_ERROR(log_msg.str());
			return -1;
		}

		s.address.setInternAddressV4(client_addr.sin_addr);
		s.address.setAddrType(AF_INET);
		s.fd = fdClient;
		s.port = ntohs(client_addr.sin_port);

	}
	else if (address.getAddrType() == AF_INET6) {
		sockaddr_in6 client_addr;
		socklen_t client_len;
		client_len = sizeof(client_addr);
		int fdClient = ::accept(this->fd, (sockaddr*)&client_addr, &client_len);

		if (fdClient < 0) {
			std::stringstream log_msg;
			log_msg << "socket accept failed: " << strerror(errno);
			LOG_WRITE_ERROR(log_msg.str());
			return -1;
		}
		s.address.setInternAddressV6(client_addr.sin6_addr);
		s.address.setAddrType(AF_INET6);
		s.fd = fdClient;
		s.port = ntohs(client_addr.sin6_port);

	}
	else {
		std::stringstream log_msg;
		log_msg << "listen socket invalid ";
		LOG_WRITE_ERROR(log_msg.str());
	}

	s.listenIp = this->listenIp;
	s.listenPort = this->listenPort;
	return 0;
}

int Socket::__bindv4(InetAddr host, int port) {
	int server_len = 0;
	struct sockaddr_in server_addr;
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(port);
	server_len = sizeof(sockaddr_in);
	server_addr.sin_addr = host.address.sin_addr;
	if (::bind(fd, (sockaddr*)&server_addr, server_len) == -1) {
		std::stringstream log_msg;
		log_msg << "socket bind to " << host.getHostAddress() << ":" << port << " failed: " << strerror(errno);
		LOG_WRITE_ERROR(log_msg.str());
		return -1;
	}
	return 0;
}

int Socket::__bindv6(InetAddr host, int port) {
	int server_len = 0;
	struct sockaddr_in6 server_addr;
	server_addr.sin6_family = AF_INET6;
	server_addr.sin6_port = htons(port);
	server_len = sizeof(struct sockaddr_in6);
	server_addr.sin6_addr = host.address.sin6_addr;

	if (::bind(fd, (sockaddr*)&server_addr, server_len) == -1) {
		std::stringstream log_msg;
		log_msg << "socket bind to " << host.getHostAddress() << ":" << port << " failed: " << strerror(errno);
		LOG_WRITE_ERROR(log_msg.str());
		return -1;
	}
	return 0;
}

int Socket::bind(InetAddr host, int port) {

	int reuseflag = 1;
	int ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (void*)&reuseflag, sizeof(reuseflag));
	if (ret < 0) {
		std::stringstream log_msg;
		log_msg << "failed to setsockopt so_reuseaddr: " << strerror(errno);
		LOG_WRITE_ERROR(log_msg.str());
		return -1;
	}

	if (host.getAddrType() == AF_INET) {
		ret = __bindv4(host, port);
	}
	else if (host.getAddrType() == AF_INET6) {
		ret = __bindv6(host, port);
	}
	this->localport = port;
	return ret;
}

/** Bind this socket to the specified port on the named host. */
int Socket::bind(const string& host, int port) {
	try {
		int ret = bind(InetAddr::getByName(host), port);
		listenIp = host;
		listenPort = port;
		return ret;
	}
	catch (const std::exception & e)
	{
		std::stringstream log_msg;
		log_msg << e.what();
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
}

int Socket::close() {
	if (fd != INVALID_SOCKET_HANDLE) {
		std::stringstream log_msg;
		log_msg << "closing socket in thread(" << gettid() << "), socketFd=" << fd;
		LOG_WRITE_INFO(log_msg.str());

		if (::close(fd) == -1) {
			std::stringstream log_msg;
			log_msg << "close socket failed: " << strerror(errno);
			LOG_WRITE_ERROR(log_msg.str());
			return -1;
		}
		fd = INVALID_SOCKET_HANDLE;
		port = 0;
		localport = -1;
	}
	return 0;
}

bool Socket::connect(const string& host, int port, uint32_t connectTimeout)
{
	m_connectTimeout = connectTimeout;
	return connectNonBock(host, port, m_connectTimeout);
}

bool Socket::connectNonBock(const string& host, int port, uint32_t milliseconds)
{
	m_connectToHost = host;
	m_connectToPort = port;
	InetAddr address;
	try {
		address = InetAddr::getByName(host);
	}
	catch (const std::exception & e)
	{
		std::stringstream log_msg;
		log_msg << e.what();
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	if (fd == INVALID_SOCKET_HANDLE) {
		if(create(m_stream, address.getAddrType())<0)
			return false;
	}
	if (fd == 0)
	{
		std::stringstream log_msg;
		log_msg << "meet fd = 0, need create socket again.";
		LOG_WRITE_ERROR(log_msg.str());
		if(create(m_stream, address.getAddrType())<0)
			return false;
	}

	int ret = false;
	if (address.getAddrType() == AF_INET) {
		ret = __connectNonBockv4(address, port, milliseconds);
	}
	else if (address.getAddrType() == AF_INET6) {
		ret = __connectNonBockv6(address, port, milliseconds);
	}
	if (!ret)
		return false;
	this->address = address;
	this->port = port;
	return true;
}

bool Socket::__connectNonBockv4(InetAddr address, int port, int milliseconds) 
{
	sockaddr_in client_addr;
	int client_len = sizeof(client_addr);
	client_addr.sin_family = AF_INET;
	client_addr.sin_addr = address.address.sin_addr;
	client_addr.sin_port = htons(port);

	int flags, n, error;
	struct timeval;
	flags = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	error = 0;

	if ((n = ::connect(fd, (struct sockaddr*) & client_addr, client_len)) < 0) {
		if (errno != EINPROGRESS) {
			std::stringstream log_msg;
			log_msg << "connect to " << address.getHostAddress() << ":" << port << " failed: " << errno << ", " << strerror(errno);
			LOG_WRITE_ERROR(log_msg.str());
			return false;
		}
	}

	if (n != 0) {
		int retval;
		struct pollfd pfd;
		pfd.fd = fd;
		pfd.events = POLLWRNORM | POLLRDNORM;
		pfd.revents = 0;

		retval = ::poll(&pfd, 1, milliseconds);  //milliseconds
		if (retval < 0) {
			std::stringstream log_msg;
			log_msg << "poll error at connect, fd " << fd << ", errno " << errno;
			LOG_WRITE_ERROR(log_msg.str());
			close(); //avoid close_wait, see http://www.chinaitpower.com/A200507/2005-07-27/174296.html
			return false;
		}
		if (retval == 0) {
			std::stringstream log_msg;
			log_msg << "poll on fd(" << fd << ") address "<<address.getHostAddress()<<":"<<port<<" at connect return timeout(" << milliseconds << " ms)";
			LOG_WRITE_ERROR(log_msg.str());
			
			close(); //avoid to recv peer response when the socket is used to next command
			return false;
		}
		socklen_t len = sizeof(error);
		if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0) {
			std::stringstream log_msg;
			log_msg << "connect to " << address.getHostAddress() << ":" << port << " failed: getsockopt error";
			LOG_WRITE_ERROR(log_msg.str());
			return false;
		}
		if (error) {
			std::stringstream log_msg;
			log_msg << "connect to " << address.getHostAddress() << ":" << port << " failed: getsockopt.error[" << error << "]";
			LOG_WRITE_ERROR(log_msg.str());
			close();
			return false;
		}
	}
	fcntl(fd, F_SETFL, flags);
	std::stringstream log_msg;
	log_msg << "connected to " << address.getHostAddress() << ":" << port << ", socketFd=" << fd;
	LOG_WRITE_INFO(log_msg.str());
	return true;
}

bool Socket::__connectNonBockv6(InetAddr address, int port, int milliseconds) {
	sockaddr_in6 client_addr;
	int client_len = sizeof(sockaddr_in6);
	bzero(&client_addr, client_len);
	client_addr.sin6_family = AF_INET6;
	client_addr.sin6_addr = address.address.sin6_addr;
	client_addr.sin6_port = htons(port);

	int flags, n, error;
	flags = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	error = 0;

	if ((n = ::connect(fd, (struct sockaddr*) & client_addr, client_len)) < 0) {
		if (errno != EINPROGRESS) {
			std::stringstream log_msg;
			log_msg << "connect to " << address.getHostAddress() << ":" << port << " failed: " << errno;
			LOG_WRITE_ERROR(log_msg.str());
			return false;
		}
	}

	if (n != 0) {
		int retval;
		struct pollfd pfd;
		pfd.fd = fd;
		pfd.events = POLLWRNORM | POLLRDNORM;
		pfd.revents = 0;

		retval = ::poll(&pfd, 1, milliseconds);  //milliseconds
		if (retval < 0) {
			std::stringstream log_msg;
			log_msg << "poll error at connect, fd " << fd << ", " << errno;
			LOG_WRITE_ERROR(log_msg.str());
			close(); //avoid close_wait, see http://www.chinaitpower.com/A200507/2005-07-27/174296.html
			return false;
		}
		if (retval == 0) {
			std::stringstream log_msg;
			log_msg << "poll on fd(" << fd << ") at connect return timeout(" << milliseconds << " ms)";
			LOG_WRITE_ERROR(log_msg.str());
			close(); //avoid to recv peer response when the socket is used to next command
			return false;
		}
		socklen_t len = sizeof(error);
		if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0) {
			std::stringstream log_msg;
			log_msg << "connect to " << address.getHostAddress() << ":" << port << " failed: getsockopt error " << error;
			LOG_WRITE_ERROR(log_msg.str());
			return false;
		}
		if (error) {
			std::stringstream log_msg;
			log_msg << "connect to " << address.getHostAddress() << ":" << port << " failed: getsockopt.error " << error;
			LOG_WRITE_ERROR(log_msg.str());
			close();
			return false;
		}
	}

	fcntl(fd, F_SETFL, flags);
	std::stringstream log_msg;
	log_msg << "connected to " << address.getHostAddress() << ":" << port << ", socketFd=" << fd;
	LOG_WRITE_INFO(log_msg.str());
	return true;
}


/**  Connects this socket to the specified port number
 on the specified host.
 */
bool Socket::__connectv4(InetAddr address, int port) {
	sockaddr_in client_addr;
	int client_len = sizeof(client_addr);
	client_addr.sin_family = AF_INET;
	client_addr.sin_addr = address.address.sin_addr;
	client_addr.sin_port = htons(port);

	if (::connect(fd, (sockaddr*)&client_addr, client_len) == -1) {
		close();
		std::stringstream log_msg;
		log_msg << "connect to " << address.getHostAddress() << ":" << port << " failed: " << errno;
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	std::stringstream log_msg;
	log_msg << "connect to " << address.getHostAddress() << ":" << port << ", socketFd=" << fd;
	LOG_WRITE_INFO(log_msg.str());
	this->address = address;
	this->port = port;
	return true;
}

bool Socket::__connectv6(InetAddr address, int port) {
	sockaddr_in6 client_addr;
	int client_len = sizeof(client_addr);
	client_addr.sin6_family = AF_INET6;
	client_addr.sin6_addr = address.address.sin6_addr;
	client_addr.sin6_port = htons(port);

	if (::connect(fd, (sockaddr*)&client_addr, client_len) == -1) {
		close();
		std::stringstream log_msg;
		log_msg << "connect to " << address.getHostAddress() << ":" << port << " failed: " << errno;
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	std::stringstream log_msg;
	log_msg << "connected to " << address.getHostAddress() << ":" << port << ", socketFd=" << fd;
	LOG_WRITE_INFO(log_msg.str());
	this->address = address;
	this->port = port;
	return true;
}

bool Socket::connect(InetAddr address, int port) {
	if (fd == INVALID_SOCKET_HANDLE) {
		create(m_stream, address.getAddrType());
	}

	if (address.getAddrType() == AF_INET) {
		return __connectv4(address, port);
	}
	else if (address.getAddrType() == AF_INET6) {
		return __connectv6(address, port);
	}
	return true;
}

/** Connects this socket to the specified port on the named host. */
bool Socket::connectOriginally(const string& host, int port) {
	m_connectToHost = host;
	m_connectToPort = port;
	try {
		return connect(InetAddr::getByName(host), port);
	}
	catch (const std::exception & e)
	{
		std::stringstream log_msg;
		log_msg << e.what();
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
}

/** Creates either a stream or a datagram socket. */
void Socket::create(bool stream) {
	m_stream = stream;
	create(true, AF_INET);
}

int Socket::create(bool stream, int addrType) {
	m_stream = stream;
	address.setAddrType(addrType);
	if (addrType == AF_INET) {
		if ((fd = ::socket(AF_INET, m_stream ? SOCK_STREAM : SOCK_DGRAM, 0)) == -1) {
			std::stringstream log_msg;
			log_msg << "socket create failed: " << errno;
			LOG_WRITE_ERROR(log_msg.str());
			return -1;
		}
	}
	else if (addrType == AF_INET6) {
		if ((fd = ::socket(AF_INET6, m_stream ? SOCK_STREAM : SOCK_DGRAM, 0)) == -1) {
			std::stringstream log_msg;
			log_msg << "socket create failed: " << errno;
			LOG_WRITE_ERROR(log_msg.str());
			return -1;
		}
	}
	return 0;
}
/** Sets the maximum queue length for incoming connection
 indications (a request to connect) to the count argument.
*/
int Socket::listen(int backlog) {
	if (::listen(fd, backlog) == -1) {
		std::stringstream log_msg;
		log_msg << "failed to listen on port " << port << ": " << errno;
		LOG_WRITE_ERROR(log_msg.str());
		return -1;
	}
	return 0;
}

/*
 *  Returns the address and port of this socket as a String.
 */
string Socket::toString() const {
	char strPort[256];
	sprintf(strPort, "%d", port);
	string ret = address.getHostAddress() + ":" + strPort;
	return ret;
}

int Socket::read2(void* buf, size_t maxlen, int secTimeout)
{
	int len_read = 0;
	unsigned char* p = (unsigned char*)buf;

	if (secTimeout > 0) {
		struct timeval tv;
		fd_set readFds;
		int ret;
		tv.tv_sec = secTimeout;
		tv.tv_usec = 0;
		FD_ZERO(&readFds);
		FD_SET(fd, &readFds);
		ret = ::select(fd + 1, &readFds, NULL, NULL, &tv);
		if (ret == -1) {
			std::stringstream log_msg;
			log_msg << "select on fd(" << fd << ") return error: " << errno;
			LOG_WRITE_ERROR(log_msg.str());

			close(); //avoid close_wait, see http://www.chinaitpower.com/A200507/2005-07-27/174296.html
			return -1;
		}
		else if (ret == 0) {
			return 0;
		}
	}

	while ((len_read = ::read(fd, p, maxlen)) < 0) {
		if (errno == EINTR) {
			std::stringstream log_msg;
			log_msg << "interrupted when reading socket";
			LOG_WRITE_ERROR(log_msg.str());
			continue;
		}
		else {
			std::stringstream log_msg;
			log_msg << "socket read return " << len_read << ": " << errno;
			LOG_WRITE_ERROR(log_msg.str());
			close(); //avoid close_wait, see http://www.chinaitpower.com/A200507/2005-07-27/174296.html
			return -1;
		}
	}

	if (len_read == 0) {
		std::stringstream log_msg;
		log_msg << "connection closed by peer when reading socket, socketFd=" << fd;
		LOG_WRITE_ERROR(log_msg.str());
		close();
	}

	return len_read;
}

int Socket::read(void* buf, size_t maxlen, int milliTimeout) {
	int len_read = 0;
	unsigned char* p = (unsigned char*)buf;

	// use poll to implement timeout
	if (milliTimeout > 0) {
		int retval;
		struct pollfd pfd;
		pfd.fd = fd;
		pfd.events = POLLIN;
		pfd.revents = 0;

		retval = ::poll(&pfd, 1, milliTimeout);  //milliseconds
		if (retval < 0) {
			//			if( errno == EINTR ){
			//				continue;
			//			}
			std::stringstream log_msg;
			log_msg << "poll error, fd " << fd << ", errno " << errno;
			LOG_WRITE_ERROR(log_msg.str());
			close(); //avoid close_wait, see http://www.chinaitpower.com/A200507/2005-07-27/174296.html
			return -1;
		}
		if ((pfd.revents & POLLERR) || (pfd.revents & POLLNVAL) || (pfd.revents & POLLHUP)) {
			std::stringstream log_msg;
			log_msg << "poll revents error ?? fd " << fd << ", pfd.revents " << pfd.revents;
			LOG_WRITE_ERROR(log_msg.str());
			close(); //avoid close_wait, see http://www.chinaitpower.com/A200507/2005-07-27/174296.html
			return -1;
		}
		if (retval == 0) {
			std::stringstream log_msg;
			log_msg << "poll on fd(" << fd << ") at read return timeout(" << milliTimeout << " ms)";
			LOG_WRITE_ERROR(log_msg.str());
			close(); //avoid to recv peer response when the socket is used to next command
			return -1;
		}
	}

	while ((len_read = ::read(fd, p, maxlen)) < 0) {
		if (errno == EINTR) {
			std::stringstream log_msg;
			log_msg << "interrupted when reading socket";
			LOG_WRITE_ERROR(log_msg.str());
			continue;
		}
		else {
			std::stringstream log_msg;
			log_msg << "socket read return " << len_read << ", " << errno;
			LOG_WRITE_ERROR(log_msg.str());
			close(); //avoid close_wait, see http://www.chinaitpower.com/A200507/2005-07-27/174296.html
			return -1;
		}
	}

	//avoid close_wait, see http://www.chinaitpower.com/A200507/2005-07-27/174296.html
	if (len_read == 0) {
		std::stringstream log_msg;
		log_msg << "connection closed by peer when reading socket, socketFd=" << fd;
		LOG_WRITE_ERROR(log_msg.str());
		close();
	}

	return len_read;
}


int Socket::readFull(void* buf, size_t len) {
	int len_read = 0;
	unsigned char* p = (unsigned char*)buf;

	while ((size_t)(p - (unsigned char*)buf) < len) {
		//len_read = ::read(fd, p, len - (p - (unsigned char *)buf));
		len_read = read(p, len - (p - (unsigned char*)buf));
		if (len_read < 0) {
			std::stringstream log_msg;
			log_msg << "socket read failed on port " << port << ", " << errno;
			LOG_WRITE_ERROR(log_msg.str());
			return -1;
		}
		if (len_read == 0) {
			break;
		}
		p += len_read;
	}

	return (p - (const unsigned char*)buf);
}

size_t Socket::writeFull(const void* buf, size_t len) {
	if (fd == INVALID_SOCKET_HANDLE) {
		std::stringstream log_msg;
		log_msg << "can not write to socketFd which is " << INVALID_SOCKET_HANDLE;
		LOG_WRITE_ERROR(log_msg.str());
		return 0;
	}

	int len_written = 0;
	const unsigned char* p = (const unsigned char*)buf;
	if (m_stream) {
		while ((size_t)(p - (const unsigned char*)buf) < len) {
			len_written = ::write(fd, p, len - (p - (const unsigned char*)buf));

			if (len_written < 0) {
				if (errno == EINTR) {
					std::stringstream log_msg;
					log_msg << "interrupted when writing socket";
					LOG_WRITE_INFO(log_msg.str());
					continue;
				}
				else {
					//
					close();
					fd = INVALID_SOCKET_HANDLE;
					break;
				}
			}
			if (len_written == 0) {
				break;
			}
			p += len_written;
		}

		return (p - (const unsigned char*)buf);
	}
	else {
		struct sockaddr_in server_addr;
		memset(&server_addr, 0, sizeof(server_addr));
		server_addr.sin_family = AF_INET;
		server_addr.sin_port = htons(m_connectToPort);
		server_addr.sin_addr.s_addr = inet_addr(m_connectToHost.c_str());
		while (true) {
			len_written = sendto(fd, buf, len, 0, (struct sockaddr*) & server_addr, sizeof(server_addr));
			if (len_written < 0) {
				if (errno == EINTR) { //interrupt
					std::stringstream log_msg;
					log_msg << "interrupted when writing socket";
					LOG_WRITE_INFO(log_msg.str());
					continue;
				}
				else {
					std::stringstream log_msg;
					log_msg << "socket write to server " << m_connectToHost << ":" << m_connectToPort << " failed. close the socket.";
					LOG_WRITE_ERROR(log_msg.str());
					close();
					break;
				}
			}
			else {
				break;
			}

		}
		return len_written;
	}
}

/** Retrive setting for SO_TIMEOUT.
 */
int Socket::getSoTimeout() const {
	return m_timeout;
}

/** Enable/disable SO_TIMEOUT with the specified timeout, in milliseconds.
 */
void Socket::setSoTimeout(int timeout) {
	m_timeout = timeout;
}

} // namespace GBDownLinker
