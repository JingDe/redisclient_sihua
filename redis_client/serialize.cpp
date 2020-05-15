#include "serialize.h"

namespace GBDownLinker {


template<>
bool save(const SerializableType& entity, DBOutStream& out)
{
	return true;
}

template<>
bool load(DBInStream& in, SerializableType& entity)
{
	return true;
}


} // namespace GBDownLinker

