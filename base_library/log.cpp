#include "log.h"
#include "glog/logging.h"

namespace
{
void SignalHandle(const char* data, int size)
{
    std::string str = data;
    LOG(ERROR) << str << std::endl;
}
}//namespace

namespace base {

CLog::CLog()
{
}

CLog::~CLog()
{
}

CLog& CLog::Instance()
{
    static CLog instance;
    return instance;
}

#ifdef USE_CPP17
// 日志文件名称 “stdout”表示日志输出到控制台中
int CLog::Init(std::string_view file)
{
    google::InitGoogleLogging(file.data());
    google::SetLogDestination(google::GLOG_INFO, file.data());
    FLAGS_stderrthreshold = google::GLOG_INFO;
    FLAGS_colorlogtostderr = true;
    FLAGS_alsologtostderr = 1;
    FLAGS_logbufsecs = 0;  // Set log output speed(s)
    FLAGS_max_log_size = 500;

    google::InstallFailureSignalHandler();
    //默认捕捉 SIGSEGV 信号信息输出会输出到 stderr，可以通过下面的方法自定义输出>方式：)
    google::InstallFailureWriter(&SignalHandle);

    return 0;
}

#else
// 日志文件名称 “stdout”表示日志输出到控制台中
int CLog::Init(const std::string& file)
{
    google::InitGoogleLogging(file.c_str());
    google::SetLogDestination(google::GLOG_INFO, file.c_str());
    FLAGS_stderrthreshold = google::GLOG_INFO;
    FLAGS_colorlogtostderr = true;
    FLAGS_alsologtostderr = 1;
    FLAGS_logbufsecs = 0;  // Set log output speed(s)
    FLAGS_max_log_size = 500;

    google::InstallFailureSignalHandler();
    //默认捕捉 SIGSEGV 信号信息输出会输出到 stderr，可以通过下面的方法自定义输出>方式：)
    google::InstallFailureWriter(&SignalHandle);

    return 0;
}

#endif

int CLog::Uninit(void)
{
    google::ShutdownGoogleLogging();
    return 0;
}

#ifdef USE_CPP17
int CLog::WriteLog(Level level, std::string_view logMsg, const char *file, int line)
{
	int glevel = google::INFO;
	if (Level::Info == level)
	{
		glevel = google::INFO;
	}
	else if (Level::Warnning == level)
	{
		glevel = google::WARNING;
	}
	else if (Level::Error == level)
	{
		glevel = google::ERROR;
	}
	else if (Level::Fatal == level)
	{
		glevel = google::FATAL;
	}
	else
	{
		glevel = google::INFO;
	}

	google::LogMessage log(file, line, glevel);
	log.stream() << logMsg.data();

	return 0;
}
#else
int CLog::WriteLog(Level level, const std::string& logMsg, const char *file, int line)
{
	int glevel = google::INFO;
	if (Level::Info == level)
	{
		glevel = google::INFO;
	}
	else if (Level::Warnning == level)
	{
		glevel = google::WARNING;
	}
	else if (Level::Error == level)
	{
		glevel = google::ERROR;
	}
	else if (Level::Fatal == level)
	{
		glevel = google::FATAL;
	}
	else
	{
		glevel = google::INFO;
	}

	google::LogMessage log(file, line, glevel);
	log.stream() << logMsg.c_str();

	return 0;
}
#endif


}//namespace base
