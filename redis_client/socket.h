#ifndef SOCKET_H_
#define SOCKET_H_
#include "inet_addr.h"
#include "redisbase.h"
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/types.h>
#include <linux/unistd.h>
#include <string>

using std::string;

namespace GBDownLinker {

const int INVALID_SOCKET_HANDLE = -1;


class Socket
{
public:
	Socket();

	virtual ~Socket();

	int accept(Socket& s);
	int bind(InetAddr host, int port);
	int bind(const string& host, int port);


	/** Closes this socket. */
	int close();

	bool connect(InetAddr address, int port);

	bool connectOriginally(const string& host, int port);

	/** Connects this socket to the specified port on the named host. */
//    bool connectNonBock(const string& host, int port, int timeOut=10);
//    bool connectNonBock(const string& host, int port, int tv_sec, int tv_usec);
	bool connectNonBock(const string& host, int port, uint32_t milliseconds);
	bool connect(const string& host, int port, uint32_t connectTimeout);

	/** Creates either a stream or a datagram socket. */
	void create(bool stream = true);  //true: tcp  false:udp
	int create(bool stream, int addrType);  //true: tcp  false:udp

	inline int getFileDescriptor() const
	{
		return fd;
	}
	/** Returns the value of this socket's address field. */
	inline InetAddr getInetAddress() const
	{
		return address;
	}

	/** Returns the value of this socket's localport field. */
	inline int getLocalPort() const { return localport; }

	/** Returns the value of this socket's port field. */
	inline int32_t getPort() const { return port; }

	/** Sets the maximum queue length for incoming connection
	indications (a request to connect) to the count argument.
	*/
	int listen(int backlog);

	/** Returns the address and port of this socket as a String.
	*/
	string toString() const;

	int read(void* buf, size_t maxlen, int milliTimeout = -1);
	int read2(void* buf, size_t maxlen, int secTimeout = -1);
	int readFull(void* buf, size_t len);

	size_t writeFull(const void* buf, size_t len);

	int getSoTimeout() const;
	void setSoTimeout(int timeout);


private:
	int __bindv4(InetAddr host, int port);
	int __bindv6(InetAddr host, int port);
	bool __connectv4(InetAddr host, int port);
	bool __connectv6(InetAddr host, int port);
	//    bool __connectNonBockv4(InetAddr host, int port, int tv_sec, int tv_usec);
	//    bool __connectNonBockv6(InetAddr host, int port, int tv_sec, int tv_usec);
	bool __connectNonBockv4(InetAddr host, int port, int milliseconds);
	bool __connectNonBockv6(InetAddr host, int port, int milliseconds);

public:
	int    fd; /** The file descriptor object for this socket. */
	string listenIp;
	int    listenPort;
	//	int    addrType;

		//as a client
	string m_connectToHost;
	int m_connectToPort;

	int m_timeout;

protected:
	/** The IP address of the remote end of this socket. */
	InetAddr address;

	/** The local port number to which this socket is connected. */
	int localport;

	/** The port number on the remote host to which this socket is connected. */
	int port;

	bool m_stream;

	int m_connectTimeout; // MilliSeconds
};

} // namespace GBDownLinker

#endif /*SOCKET_H_*/
