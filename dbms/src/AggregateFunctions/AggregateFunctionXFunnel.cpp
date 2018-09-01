#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionXFunnel.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

template <bool repeated = false>
AggregateFunctionPtr createAggregateFunctionXFunnel(const std::string & name, const DataTypes & arguments, const Array & params)
{
    if (params.size() < 3 || params.size() > 4)
        throw Exception{"Aggregate function " + name + " requires 3 - 4 parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    if (arguments.size() < 2)
        throw Exception("Aggregate function " + name + " requires one column array argument and at least one event condition.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (arguments.size() > AggregateFunctionXFunnelData::max_events + 1)
        throw Exception("Too many event arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionXFunnel<repeated>>(arguments, params);
}

}

void registerAggregateFunctionXFunnel(AggregateFunctionFactory & factory)
{
    factory.registerFunction("xFunnel", createAggregateFunctionXFunnel<false>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("xRepeatedFunnel", createAggregateFunctionXFunnel<true>, AggregateFunctionFactory::CaseInsensitive);
}

}
