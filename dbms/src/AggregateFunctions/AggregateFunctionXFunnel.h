#pragma once

#include <iostream>
#include <sstream>
#include <unordered_set>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ArenaAllocator.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <common/logger_useful.h>

#include <Core/Field.h>
#include <boost/algorithm/string/split.hpp>
#include <ext/range.h>

#include <AggregateFunctions/IAggregateFunction.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

struct TimestampEvent
{
    UInt32 timestamp;
    UInt16 day;
    Tuple tuple;
    UInt8 event;

    TimestampEvent(const UInt32 & timestamp_, const UInt16 & day_, Tuple & tuple_, const UInt8 & event_)
        : timestamp{timestamp_}, day{day_}, tuple{std::move(tuple_)}, event{event_}
    {
    }
};

struct ComparePairFirst final
{
    bool operator()(const TimestampEvent & lhs, const TimestampEvent & rhs) const
    {
        return lhs.timestamp < rhs.timestamp;
    }
};


static constexpr size_t bytes_on_stack = 4096;
//using TimestampEvents = PODArray<TimestampEvent, bytes_on_stack>;
using TimestampEvents = std::vector<TimestampEvent>;
using validataFunc = std::function<bool(const Tuple & tuple, const Tuple & pre_tuple)>;
using Comparator = ComparePairFirst;

struct AggregateFunctionXFunnelData
{
    static constexpr auto max_events = 32;

    bool sorted = true;
    TimestampEvents events_list;


    size_t size() const
    {
        return events_list.size();
    }

    void add(const UInt32 & timestamp, const UInt16 & day, Tuple & node, UInt8 event)
    {
        // Since most events should have already been sorted by timestamp.
        if (sorted && events_list.size() > 0 && events_list.back().timestamp > timestamp)
        {
            sorted = false;
        }
        events_list.emplace_back(timestamp, day, node, event);
    }

    void merge(const AggregateFunctionXFunnelData & other)
    {
        const auto size = events_list.size();
        //        events_list.insert(std::begin(other.events_list), std::end(other.events_list));
        events_list.insert(events_list.end(), other.events_list.begin(), other.events_list.end());

        /// either sort whole container or do so partially merging ranges afterwards
        if (!sorted && !other.sorted)
            std::stable_sort(std::begin(events_list), std::end(events_list), Comparator{});
        else
        {
            const auto begin = std::begin(events_list);
            const auto middle = std::next(begin, size);
            const auto end = std::end(events_list);

            if (!sorted)
                std::stable_sort(begin, middle, Comparator{});

            if (!other.sorted)
                std::stable_sort(middle, end, Comparator{});

            std::inplace_merge(begin, middle, end, Comparator{});
        }

        sorted = true;
    }

    void sort()
    {
        if (!sorted)
        {
            std::stable_sort(std::begin(events_list), std::end(events_list), Comparator{});
            sorted = true;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        writeBinary(events_list.size(), buf);

        for (const auto & events : events_list)
        {
            writeBinary(events.timestamp, buf);
            writeBinary(events.day, buf);
            auto & tuple = events.tuple;
            writeBinary(tuple, buf);
            writeBinary(events.event, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(sorted, buf);

        size_t size;
        readBinary(size, buf);

        /// TODO Protection against huge size

        events_list.clear();
        events_list.reserve(size);

        UInt32 timestamp;
        UInt16 day;
        UInt8 event;

        for (size_t i = 0; i < size; ++i)
        {
            readBinary(timestamp, buf);
            readBinary(day, buf);

            Tuple node;
            readTuple(node, buf);

            readBinary(event, buf);
            events_list.emplace_back(timestamp, day, node, event);
        }
    }
};


/** Calculates the max event level in a sliding window.
  * The max size of events is 32, that's enough for funnel analytics
  * repeated indicates is there any repeated conditions, default to false
  * Usage:
  * - xFunnel(windowSize, 2, rule)( (timestamp, col1, col2....) , cond1, cond2, cond3, ....)
  */
template <bool repeated = false>
class AggregateFunctionXFunnel final : public IAggregateFunctionDataHelper<AggregateFunctionXFunnelData, AggregateFunctionXFunnel<repeated>>
{
private:
    UInt32 window;
    UInt8 events_size;
    UInt8 max_output_idx;
    String rule_arg{""};
    size_t tuple_size;

    DataTypes dataTypes;

    using validataIdxFuncs = std::vector<std::pair<UInt8, validataFunc>>;
    std::vector<validataIdxFuncs> validators;

    using Indexs = std::vector<UInt16>;
    using DayIndexs = std::vector<Indexs>;

    // 多维数组，第一维度表示某天开头的所有可能事件流序列
    using DayIndexsPattern = std::vector<std::vector<DayIndexs>>;

    /// 匹配算法
    ALWAYS_INLINE DayIndexs getFunnelIndexArray(const AggregateFunctionXFunnelData & data) const
    {
        if (data.size() == 0)
            return {};

        const_cast<AggregateFunctionXFunnelData &>(data).sort();

        // 返回多个漏斗匹配事件流，按天分组
        UInt16 first_day = data.events_list.front().day;
        int count = data.events_list.back().day - first_day + 1;
        DayIndexs res(count);
        DayIndexsPattern patterns(count, std::vector<DayIndexs>(events_size));

//        LOG_TRACE(&Logger::get("xFunnel"), "list=>" << data.events_list.size() <<  ",day_size" << count << "~" << "min=>" << first_day << "max=>" <<  data.events_list.back().day);


        bool ok;
        int tmp_idx;

        /// let's fuck the BT rules
        /// 4，每一天取步骤数最大的路径，如果最大步骤数有多条
        /// 4.1最大步骤数 等于 漏斗最大步骤，取最大步骤最靠 前 的
        /// 4.2最大步骤数 小于 漏斗最大步骤，取最大步骤最靠 后 的
        /// 4.3如果最大步骤时间也一样，取最开始时间靠后的

        for (UInt16 idx = 0; idx < data.events_list.size(); idx++)
        {
            const auto & event = data.events_list[idx];
            const auto & timestamp = event.timestamp;
            const auto & tuple = event.tuple;
            const auto & event_idx = event.event;
            const auto & day = event.day;


            if (event_idx == 0)
            {
                if (!res[day - first_day].empty())
                {
                    continue;
                }
                Indexs tmp = Indexs{idx};
                patterns[day - first_day][0].push_back(std::move(tmp));
            }

            else
            {
                for (UInt16 day_idx = 0; day_idx < patterns.size(); ++day_idx)
                {
                    if (!res[day_idx].empty())
                        continue;

                    tmp_idx = patterns[day_idx].front().empty() ? -1 : patterns[day_idx].front().back().front();
                    if (tmp_idx != -1 && data.events_list[tmp_idx].timestamp + window < timestamp)
                    {
                        continue;
                    }

                    for (const auto & pre_indexes : patterns[day_idx][event_idx - 1])
                    {
                        if (pre_indexes.size() < 1)
                            break;

                        tmp_idx = pre_indexes.front();
                        if (data.events_list[tmp_idx].timestamp + window >= timestamp)
                        {
                            ok = true;

                            const auto & idx_funcs = validators[event_idx];
                            for (const auto & idxFunc : idx_funcs)
                            {
                                const auto & pre_tuple = data.events_list[pre_indexes[idxFunc.first]].tuple;
                                if (!idxFunc.second(tuple, pre_tuple))
                                {
                                    ok = false;
                                    break;
                                }
                            }

                            if (ok)
                            {
                                //如果是最后一个，只需要取最后一个！！
                                Indexs current_indexes = pre_indexes;
                                current_indexes.push_back(idx);
                                patterns[day_idx][event_idx].push_back(std::move(current_indexes));
                            }
                        }
                    }
                    /// now we reach the final goal~ but we should continue ...
                    if (event_idx + 1 == events_size && res[day_idx].empty() && !patterns[day_idx][event_idx].empty())
                    {
                        res[day_idx] = patterns[day_idx][event_idx].back();
                        tmp_idx = patterns[day_idx][event_idx].back().front();
                        auto iter =  patterns[day_idx][event_idx].rbegin();
                        while ((++iter) != patterns[day_idx][event_idx].rend() && iter->back() ==  res[day_idx].back())
                        {
                            if (iter->front() > tmp_idx)
                            {
                               tmp_idx = iter->front();
                               res[day_idx] = *iter;
                            }
                        }
//                        LOG_TRACE(&Logger::get("xFunnel"), "resulting=> " << day_idx << "size=>" << res[day_idx].size());
                        count--;
                    }
                }
            }
        }


        /// for the days never reach final goal, let's gather them all.
        if (count != 0)
        {
            for (UInt16 day_idx = 0; day_idx < patterns.size(); ++day_idx)
            {
//                LOG_TRACE(&Logger::get("xFunnel"), "filling=> " << day_idx << "size=>" << res[day_idx].size());

                if (!res[day_idx].empty())
                    continue;

                /// return the rightest event sequences
                for (size_t event = events_size - 1; event > 0; --event)
                {
                    if (!patterns[day_idx][event - 1].empty()) {
                        // 4.3 规则
                        res[day_idx] = patterns[day_idx][event - 1].back();
                        if (patterns[day_idx][event - 1].size() > 1) {
                            tmp_idx = patterns[day_idx][event - 1].back().front();
                            auto iter =  patterns[day_idx][event - 1].rbegin();
                            while ((++iter) != patterns[day_idx][event - 1].rend() && iter->back() ==  res[day_idx].back())
                            {
                                if (iter->front() > tmp_idx)
                                {
                                   tmp_idx = iter->front();
                                   res[day_idx] = *iter;
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }
        return std::move(res);
    }

public:
    String getName() const override
    {
        return !repeated ? "xFunnel" : "xRepeatedFunnel";
    }

    AggregateFunctionXFunnel(const DataTypes & arguments, const Array & params)
    {
        window = params.at(0).safeGet<UInt64>();

        // [min_ouput_idx, max_output_idx]
        max_output_idx = params.at(1).safeGet<UInt64>() - 1;
        if (max_output_idx < 1)
            throw Exception{"Invalid number of " + toString(max_output_idx+1) + ", must greater than 2"};

        const auto col_arg = arguments[0].get();
        auto tuple_args = typeid_cast<const DataTypeTuple *>(col_arg);
        if (!tuple_args)
            throw Exception{
            "Illegal type " + col_arg->getName() + " of first argument of aggregate function " + getName() + ", must be tuple"};
        tuple_size = tuple_args->getElements().size();
        events_size = arguments.size() - 1;
        validators.resize(events_size);

        for (const auto & i : ext::range(1, arguments.size()))
        {
            const auto & cond_arg = arguments[i].get();
            if (!typeid_cast<const DataTypeUInt8 *>(cond_arg))
                throw Exception{"Illegal type " + cond_arg->getName() + " of argument " + toString(i + 1) + " of aggregate function "
                        + getName() + ", must be UInt8",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        // (timestamp, day, a,b,c,d,...) ) =>  (timestamp, day, a) for max_output_idx == 3
        const auto & tupleType = typeid_cast<const DataTypeTuple *>(arguments[0].get());
        const auto & time_arg = static_cast<const DataTypeUInt32 *>(tupleType->getElements()[0].get());
        if (!time_arg)
            throw Exception{"Illegal type " + time_arg->getName() + " 1st of first tuple argument of aggregate function " + getName()
                    + ", must be DateTime or UInt32"};

        const auto & day_arg = static_cast<const DataTypeUInt16 *>(tupleType->getElements()[1].get());
        if (!day_arg)
            throw Exception{"Illegal type " + day_arg->getName() + " 2st of first tuple argument of aggregate function " + getName()
                    + ", must be Date or UInt16"};


        for (const auto & idx : ext::range(0, max_output_idx + 1))
        {
            dataTypes.emplace_back(tupleType->getElements()[idx]);
        }

        if (params.size() > 2)
        {
            rule_arg = params.at(2).safeGet<String>();
            if (!rule_arg.empty())
                initRule();
        }
        LOG_TRACE(&Logger::get("xFunnel"), "tuple_size " << tuple_size << " rule_args " << rule_arg << " window " << window);
    }

    // 初始化rule规则
    ALWAYS_INLINE void initRule()
    {
        std::vector<String> ruleStrs;
        // eg 1.1=2.1,3.2=2.2
        boost::split(ruleStrs, rule_arg, [](char c) { return c == ','; });
        if (ruleStrs.size() < 1)
            return;

        for (String & ruleStr : ruleStrs)
        {
            std::vector<String> ruleKvs;
            boost::split(ruleKvs, ruleStr, [](char c) { return c == '='; });

            // eg [1.1,2.2]
            if (ruleKvs.size() != 2)
                throw Exception{"Illegal rule " + ruleStr, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            std::vector<String> ruleKeys;
            std::vector<String> ruleValues;
            boost::split(ruleKeys, ruleKvs[0], [](char c) { return c == '.'; });
            boost::split(ruleValues, ruleKvs[1], [](char c) { return c == '.'; });

            int previous_event_level = std::atoi(ruleKeys[0].c_str());
            int previous_label_idx = std::atoi(ruleKeys[1].c_str());

            int current_event_level = std::atoi(ruleValues[0].c_str());
            int current_label_idx = std::atoi(ruleValues[1].c_str());

            if (previous_event_level < current_event_level)
            {
                addRule(previous_event_level - 1, previous_label_idx - 3, current_event_level - 1, current_label_idx - 3);
            }
            else
            {
                addRule(current_event_level - 1, current_label_idx - 3, previous_event_level - 1, previous_label_idx - 3);
            }
        }
    }

    ALWAYS_INLINE void addRule(const size_t & previous_event_level,
                               const size_t & previous_label_idx,
                               const size_t & current_event_level,
                               const size_t & current_label_idx)
    {
        auto func = [=](const Tuple & current_label, const Tuple & pre_label) {
            const TupleBackend & current_arr = current_label.toUnderType();
            const TupleBackend & previous_arr = pre_label.toUnderType();

            return current_arr[current_label_idx] == previous_arr[previous_label_idx];
        };
        validators[current_event_level].push_back(std::make_pair(previous_event_level, func));
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(dataTypes)));
    }

    void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        if constexpr (!repeated)
        {
            for (const auto idx : ext::range(1, events_size + 1))
            {
                auto event = static_cast<const ColumnVector<UInt8> *>(columns[idx])->getData()[row_num];
                if (event)
                {
                    auto labelCol = static_cast<const ColumnTuple *>(columns[0]);

                    /// 这里将timestamp和day抽离出来
                    Tuple tuple = Tuple(TupleBackend(tuple_size - 2));
                    auto & arr = tuple.toUnderType();
                    for (const auto i : ext::range(2, tuple_size))
                    {
                        labelCol->getColumn(i).get(row_num, arr[i - 2]);
                    }
                    this->data(place).add(labelCol->getColumn(0).getInt(row_num), labelCol->getColumn(1).getInt(row_num), tuple, idx - 1);
                    break;
                }
            }
        }
        else
        {
            for (auto idx = events_size; idx >= 1; --idx)
            {
                auto event = static_cast<const ColumnVector<UInt8> *>(columns[idx])->getData()[row_num];
                if (event)
                {
                    auto labelCol = static_cast<const ColumnTuple *>(columns[0]);

                    /// 这里将timestamp和day抽离出来
                    Tuple tuple = Tuple(TupleBackend(tuple_size - 2));
                    auto & arr = tuple.toUnderType();
                    for (const auto i : ext::range(2, tuple_size))
                    {
                        labelCol->getColumn(i).get(row_num, arr[i - 2]);
                    }
                    this->data(place).add(labelCol->getColumn(0).getInt(row_num), labelCol->getColumn(1).getInt(row_num), tuple, idx - 1);
                }
            }
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        const auto & funnel_index_array = getFunnelIndexArray(this->data(place));
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        int count = funnel_index_array.size();
        for (const auto & funnel_index : funnel_index_array)
        {
            if (funnel_index.empty())
            {
                count --;
                continue;
            }
            //static_cast<ColumnUInt8 &>(static_cast<ColumnArray &>(to).getData()).getData();
            auto & arr_tuple_to = static_cast<ColumnArray &>(arr_to.getData());
            auto & offset_tuple_to = arr_tuple_to.getOffsets();

            for (const auto & index : funnel_index)
            {
                auto & timestamp_event = this->data(place).events_list[index];
                auto & tuple_data = static_cast<ColumnTuple &>(arr_tuple_to.getData());

                tuple_data.getColumn(0).insert(static_cast<UInt64>(timestamp_event.timestamp));
                tuple_data.getColumn(1).insert(static_cast<UInt64>(timestamp_event.day));
                for (size_t idx = 2; idx <= max_output_idx; idx++)
                {
                    tuple_data.getColumn(idx).insert(timestamp_event.tuple.toUnderType()[idx - 2]);
                }
            }
            offset_tuple_to.push_back(offset_tuple_to.size() == 0 ? funnel_index.size() : offset_tuple_to.back() + funnel_index.size());
        }
        offsets_to.push_back(offsets_to.size() == 0 ? count : offsets_to.back() + count);
    }

    const char * getHeaderFilePath() const override
    {
        return __FILE__;
    }
};
}
