// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "io/local_file_reader.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exec/format/table/iceberg_reader.h"

#include "vec/utils/util.hpp"

namespace doris {
namespace vectorized {

class IcebergReaderEqualityDeleteTest : public testing::Test {
public:
    IcebergReaderEqualityDeleteTest() {}
};

TEST_F(IcebergReaderEqualityDeleteTest, case_1) {
    // select id,order_id,adowner_id,order_time,modify_time from union_sku where order_month = '2021-06' and order_id >= 205315548454  and order_id <= 205315548455 order by order_id, modify_time;
    auto state = doris::Status::OK();
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);


    doris::RuntimeState _state(doris::TUniqueId(), doris::TQueryOptions(),
                                     doris::TQueryGlobals(), nullptr);
    _state.init_mem_trackers();
    KVCache<std::string> _kv_cache;
    RuntimeProfile* _profile = _state.obj_pool()->add(new RuntimeProfile("IcebergReaderEqualityDeleteTest"));

    std::string t_desc_table_json =
        R"|({"1":{"lst":["rec",11,{"1":{"i32":0},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":-1},"5":{"i32":8},"6":{"i32":0},"7":{"i32":0},"8":{"str":"id"},"9":{"i32":0},"10":{"tf":1},"11":{"i32":1}},{"1":{"i32":1},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":-1},"5":{"i32":16},"6":{"i32":0},"7":{"i32":1},"8":{"str":"order_id"},"9":{"i32":1},"10":{"tf":1},"11":{"i32":2}},{"1":{"i32":2},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}}},"4":{"i32":-1},"5":{"i32":32},"6":{"i32":0},"7":{"i32":2},"8":{"str":"adowner_id"},"9":{"i32":2},"10":{"tf":1},"11":{"i32":34}},{"1":{"i32":3},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":-1},"5":{"i32":48},"6":{"i32":0},"7":{"i32":3},"8":{"str":"order_time"},"9":{"i32":3},"10":{"tf":1},"11":{"i32":75}},{"1":{"i32":4},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":-1},"5":{"i32":64},"6":{"i32":0},"7":{"i32":4},"8":{"str":"modify_time"},"9":{"i32":4},"10":{"tf":1},"11":{"i32":79}},{"1":{"i32":5},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}}},"4":{"i32":-1},"5":{"i32":80},"6":{"i32":0},"7":{"i32":5},"8":{"str":"order_month"},"9":{"i32":5},"10":{"tf":1},"11":{"i32":101}},{"1":{"i32":6},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":-1},"5":{"i32":8},"6":{"i32":0},"7":{"i32":0},"8":{"str":""},"9":{"i32":0},"10":{"tf":1},"11":{"i32":-1}},{"1":{"i32":7},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":-1},"5":{"i32":32},"6":{"i32":0},"7":{"i32":2},"8":{"str":""},"9":{"i32":2},"10":{"tf":1},"11":{"i32":-1}},{"1":{"i32":8},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":-1},"5":{"i32":16},"6":{"i32":0},"7":{"i32":1},"8":{"str":""},"9":{"i32":1},"10":{"tf":1},"11":{"i32":-1}},{"1":{"i32":9},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}}},"4":{"i32":-1},"5":{"i32":48},"6":{"i32":0},"7":{"i32":3},"8":{"str":""},"9":{"i32":3},"10":{"tf":1},"11":{"i32":-1}},{"1":{"i32":10},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":-1},"5":{"i32":64},"6":{"i32":0},"7":{"i32":4},"8":{"str":""},"9":{"i32":4},"10":{"tf":1},"11":{"i32":-1}}]},"2":{"lst":["rec",2,{"1":{"i32":0},"2":{"i32":96},"3":{"i32":1},"4":{"i64":56087},"5":{"i32":6}},{"1":{"i32":1},"2":{"i32":80},"3":{"i32":1},"5":{"i32":5}}]},"3":{"lst":["rec",1,{"1":{"i64":56087},"2":{"i32":8},"3":{"i32":101},"4":{"i32":0},"7":{"str":"union_sku"},"8":{"str":"cps"},"18":{"rec":{"1":{"str":"cps"},"2":{"str":"union_sku"},"3":{"map":["str","str",0,{}]}}}}]}})|";
    TDescriptorTable t_desc_table = apache::thrift::from_json_string<doris::TDescriptorTable>(t_desc_table_json);
    DescriptorTbl* desc_tbl;
    DescriptorTbl::create(_state.obj_pool(), t_desc_table, &desc_tbl);
    _state.set_desc_tbl(desc_tbl);
    auto tuple_desc = const_cast<doris::TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    doris::RowDescriptor row_desc(tuple_desc, false);

    std::string scan_param_json = R"|({"1":{"i32":0},"2":{"i32":6},"4":{"i32":-1},"5":{"i32":0},"6":{"i32":100},"7":{"lst":["rec",6,{"1":{"i32":0},"2":{"tf":1}},{"1":{"i32":1},"2":{"tf":1}},{"1":{"i32":2},"2":{"tf":1}},{"1":{"i32":3},"2":{"tf":1}},{"1":{"i32":4},"2":{"tf":1}},{"1":{"i32":5},"2":{"tf":0}}]},"11":{"map":["i32","rec",6,{"0":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}},"1":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}},"2":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":15},"2":{"i32":-1}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}},"3":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}},"4":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}},"5":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":15},"2":{"i32":-1}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}}}]},"18":{"lst":["i32",5,0,1,33,74,78]}})|";
    TFileScanRangeParams t_scan_param = apache::thrift::from_json_string<doris::TFileScanRangeParams>(scan_param_json);
    // with equality_detete to 0
    std::string scan_range_json = R"|({"2":{"str":"./be/test/exec/test_data/iceberg_reader/00000-0-3b867be4-81f7-469d-a29f-49d0a031d4b2-01085.parquet"},"3":{"i64":4},"4":{"i64":29113},"6":{"lst":["str",1,"2021-06"]},"7":{"lst":["str",1,"order_month"]},"8":{"rec":{"1":{"str":"iceberg"},"2":{"rec":{"1":{"i32":2},"2":{"i32":1},"3":{"lst":["rec",3,{"1":{"str":"./be/test/exec/test_data/iceberg_reader/00002-0-c66a6a61-64d2-4928-ba21-72b56436ccb4-10429.parquet"},"2":{"i64":0},"3":{"i64":0},"4":{"i32":1}},{"1":{"str":"./be/test/exec/test_data/iceberg_reader/00000-0-3b867be4-81f7-469d-a29f-49d0a031d4b2-10536.parquet"},"4":{"i32":2},"5":{"lst":["i32",4,1,34,2,101]}},{"1":{"str":"./be/test/exec/test_data/iceberg_reader/00000-0-3b867be4-81f7-469d-a29f-49d0a031d4b2-10539.parquet"},"2":{"i64":0},"3":{"i64":0},"4":{"i32":1}}]}}}}}})|";
    TFileRangeDesc t_scan_range = apache::thrift::from_json_string<doris::TFileRangeDesc>(scan_range_json);

    auto p_reader = new ParquetReader(nullptr, t_scan_param, t_scan_range, 4000, &ctz);
    p_reader->open();

    IcebergTableReader* iceberg_reader = new IcebergTableReader((GenericReader*)p_reader, _profile, &_state,
                                               t_scan_param, t_scan_range, _kv_cache);

    std::vector<std::string> file_col_names{"id", "order_id", "adowner_id", "order_time", "modify_time"};
    std::unordered_map<int, std::string> col_id_name_map{
        {1, "id"}, {2, "order_id"}, {34, "adowner_id"}, {75, "order_time"}, {79, "modify_time"}
    };

    std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
    colname_to_value_range["id"] = ColumnValueRange<TYPE_BIGINT>("id");
    colname_to_value_range["order_id"] = ColumnValueRange<TYPE_BIGINT>("order_id", 205315548454L, 205315548455L, false, -1, -1);
    colname_to_value_range["modify_time"] = ColumnValueRange<TYPE_DATETIME>("modify_time");
    colname_to_value_range["adowner_id"] = ColumnValueRange<TYPE_STRING>("adowner_id");
    colname_to_value_range["order_time"] = ColumnValueRange<TYPE_DATETIME>("order_time");
    ColumnValueRangeType col_range_moth = ColumnValueRange<TYPE_STRING>("order_month");
    const std::string month = "2021-06";
    auto sv_month = StringValue(month.c_str(), month.length());
    ColumnValueRange<TYPE_STRING>::add_fixed_value_range(std::get<7>(col_range_moth), reinterpret_cast<typename PrimitiveTypeTraits<TYPE_STRING>::CppType*>(&sv_month));
    colname_to_value_range["order_month"] = col_range_moth;

    std::string expr_json =
        R"|({"1":{"lst":["rec",11,{"1":{"i32":6},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"3":{"i32":2},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"1":{"str":""},"2":{"str":"and"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"5":{"tf":0},"7":{"str":"and(BOOLEAN, BOOLEAN)"},"11":{"i64":0},"13":{"tf":0}}},"29":{"tf":1}},{"1":{"i32":6},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"3":{"i32":2},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"1":{"str":""},"2":{"str":"and"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"5":{"tf":0},"7":{"str":"and(BOOLEAN, BOOLEAN)"},"11":{"i64":0},"13":{"tf":0}}},"29":{"tf":1}},{"1":{"i32":2},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"3":{"i32":9},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"eq"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"5":{"tf":0},"7":{"str":"eq(TEXT, TEXT)"},"9":{"rec":{"1":{"str":"doris::Operators::eq_string_val_string_val"}}},"11":{"i64":0},"13":{"tf":1}}},"28":{"i32":23},"29":{"tf":1}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":5},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1},"29":{"tf":1}},{"1":{"i32":17},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":15},"2":{"i32":-1}}}}]}}},"4":{"i32":0},"16":{"rec":{"1":{"str":"2021-06"}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":2},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"3":{"i32":14},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"ge"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"5":{"tf":0},"7":{"str":"ge(BIGINT, BIGINT)"},"9":{"rec":{"1":{"str":"doris::Operators::ge_big_int_val_big_int_val"}}},"11":{"i64":0},"13":{"tf":1}}},"28":{"i32":6},"29":{"tf":1}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":1},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1},"29":{"tf":1}},{"1":{"i32":9},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"10":{"rec":{"1":{"i64":205315548454}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":2},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"3":{"i32":12},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"le"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"5":{"tf":0},"7":{"str":"le(BIGINT, BIGINT)"},"9":{"rec":{"1":{"str":"doris::Operators::le_big_int_val_big_int_val"}}},"11":{"i64":0},"13":{"tf":1}}},"28":{"i32":6},"29":{"tf":1}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":1},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1},"29":{"tf":1}},{"1":{"i32":9},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"10":{"rec":{"1":{"i64":205315548455}}},"20":{"i32":-1},"29":{"tf":0}}]}})|";
    doris::TExpr exprx = apache::thrift::from_json_string<doris::TExpr>(expr_json);
    doris::vectorized::VExprContext* context = nullptr;
    doris::vectorized::VExpr::create_expr_tree(_state.obj_pool(), exprx, &context);
    state = context->prepare(&_state, row_desc);
    ASSERT_TRUE(state.ok());
    state = context->open(&_state);
    ASSERT_TRUE(state.ok());

    {
        using namespace apache::thrift::transport;
        using namespace apache::thrift::protocol;
        auto* buffer = new TMemoryBuffer;
        std::shared_ptr<TTransport> trans(buffer);
        TJSONProtocol protocol(trans);
        exprx.write(&protocol);
        uint8_t* buf;
        uint32_t size;
        buffer->getBuffer(&buf, &size);
        std::string expr_json = std::string((char*)buf, (unsigned int)size);
        VLOG_NOTICE << "TExpr: " << expr_json;
    } 

    state = iceberg_reader->init_reader(file_col_names, col_id_name_map, &colname_to_value_range, context);
    ASSERT_TRUE(state.OK());
    state = iceberg_reader->init_row_filters(t_scan_range);
    ASSERT_TRUE(state.OK());

    auto partition_slot_desc = const_cast<doris::SlotDescriptor*>(desc_tbl->get_slot_descriptor(5));
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    partition_columns["order_month"] = std::make_tuple("2021-06", partition_slot_desc);  
    std::unordered_map<std::string, VExprContext*> missing_columns;
    iceberg_reader->set_fill_columns(partition_columns, missing_columns);

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    Block* block = new Block();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type =
                vectorized::DataTypeFactory::instance().create_data_type(slot_desc->type(), true);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }

    size_t read_rows = 0;
    bool cur_reader_eof;
    state = iceberg_reader->get_next_block(block, &read_rows, &cur_reader_eof);

    std::cout << block->dump_data();
    for (auto& col : block->get_columns_with_type_and_name()) {
        ASSERT_EQ(col.column->size(), 0);
    }

    context->close(&_state);
    delete block;
    delete p_reader;
}

TEST_F(IcebergReaderEqualityDeleteTest, case_2) {
    // select id,order_id,adowner_id,order_time,modify_time from union_sku where order_month = '2022-06' and order_id = 258696459711 order by order_id, id;
    auto state = doris::Status::OK();
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);


    doris::RuntimeState _state(doris::TUniqueId(), doris::TQueryOptions(),
                                     doris::TQueryGlobals(), nullptr);
    _state.init_mem_trackers();
    KVCache<std::string> _kv_cache;
    RuntimeProfile* _profile = _state.obj_pool()->add(new RuntimeProfile("IcebergReaderEqualityDeleteTest"));

    std::string t_desc_table_json =
        R"|({"1":{"lst":["rec",11,{"1":{"i32":0},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":-1},"5":{"i32":8},"6":{"i32":0},"7":{"i32":0},"8":{"str":"id"},"9":{"i32":0},"10":{"tf":1},"11":{"i32":1}},{"1":{"i32":1},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":-1},"5":{"i32":16},"6":{"i32":0},"7":{"i32":1},"8":{"str":"order_id"},"9":{"i32":1},"10":{"tf":1},"11":{"i32":2}},{"1":{"i32":2},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}}},"4":{"i32":-1},"5":{"i32":32},"6":{"i32":0},"7":{"i32":2},"8":{"str":"adowner_id"},"9":{"i32":2},"10":{"tf":1},"11":{"i32":34}},{"1":{"i32":3},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":-1},"5":{"i32":48},"6":{"i32":0},"7":{"i32":3},"8":{"str":"order_time"},"9":{"i32":3},"10":{"tf":1},"11":{"i32":75}},{"1":{"i32":4},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":-1},"5":{"i32":64},"6":{"i32":0},"7":{"i32":4},"8":{"str":"modify_time"},"9":{"i32":4},"10":{"tf":1},"11":{"i32":79}},{"1":{"i32":5},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}}},"4":{"i32":-1},"5":{"i32":80},"6":{"i32":0},"7":{"i32":5},"8":{"str":"order_month"},"9":{"i32":5},"10":{"tf":1},"11":{"i32":101}},{"1":{"i32":6},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":-1},"5":{"i32":8},"6":{"i32":0},"7":{"i32":0},"8":{"str":""},"9":{"i32":0},"10":{"tf":1},"11":{"i32":-1}},{"1":{"i32":7},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":-1},"5":{"i32":32},"6":{"i32":0},"7":{"i32":2},"8":{"str":""},"9":{"i32":2},"10":{"tf":1},"11":{"i32":-1}},{"1":{"i32":8},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":-1},"5":{"i32":16},"6":{"i32":0},"7":{"i32":1},"8":{"str":""},"9":{"i32":1},"10":{"tf":1},"11":{"i32":-1}},{"1":{"i32":9},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}}},"4":{"i32":-1},"5":{"i32":48},"6":{"i32":0},"7":{"i32":3},"8":{"str":""},"9":{"i32":3},"10":{"tf":1},"11":{"i32":-1}},{"1":{"i32":10},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":-1},"5":{"i32":64},"6":{"i32":0},"7":{"i32":4},"8":{"str":""},"9":{"i32":4},"10":{"tf":1},"11":{"i32":-1}}]},"2":{"lst":["rec",2,{"1":{"i32":0},"2":{"i32":96},"3":{"i32":1},"4":{"i64":56087},"5":{"i32":6}},{"1":{"i32":1},"2":{"i32":80},"3":{"i32":1},"5":{"i32":5}}]},"3":{"lst":["rec",1,{"1":{"i64":56087},"2":{"i32":8},"3":{"i32":101},"4":{"i32":0},"7":{"str":"union_sku"},"8":{"str":"cps"},"18":{"rec":{"1":{"str":"cps"},"2":{"str":"union_sku"},"3":{"map":["str","str",0,{}]}}}}]}})|";
    TDescriptorTable t_desc_table = apache::thrift::from_json_string<doris::TDescriptorTable>(t_desc_table_json);
    DescriptorTbl* desc_tbl;
    DescriptorTbl::create(_state.obj_pool(), t_desc_table, &desc_tbl);
    _state.set_desc_tbl(desc_tbl);
    auto tuple_desc = const_cast<doris::TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    doris::RowDescriptor row_desc(tuple_desc, false);

    std::string scan_param_json = R"|({"1":{"i32":0},"2":{"i32":6},"4":{"i32":-1},"5":{"i32":0},"6":{"i32":100},"7":{"lst":["rec",6,{"1":{"i32":0},"2":{"tf":1}},{"1":{"i32":1},"2":{"tf":1}},{"1":{"i32":2},"2":{"tf":1}},{"1":{"i32":3},"2":{"tf":1}},{"1":{"i32":4},"2":{"tf":1}},{"1":{"i32":5},"2":{"tf":0}}]},"11":{"map":["i32","rec",6,{"0":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}},"1":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}},"2":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":15},"2":{"i32":-1}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}},"3":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}},"4":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":10}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}},"5":{"1":{"lst":["rec",1,{"1":{"i32":15},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":15},"2":{"i32":-1}}}}]}}},"4":{"i32":0},"20":{"i32":-1},"29":{"tf":1}}]}}}]},"18":{"lst":["i32",5,0,1,33,74,78]}})|";
    TFileScanRangeParams t_scan_param = apache::thrift::from_json_string<doris::TFileScanRangeParams>(scan_param_json);
    // with equality_detete to 0
    std::string scan_range_json = R"|({"2":{"str":"./be/test/exec/test_data/iceberg_reader/00001-0-3cdfaaeb-1a8a-4c75-9f86-61c3bd330d90-00415.parquet"},"3":{"i64":4},"4":{"i64":31489},"6":{"lst":["str",1,"2022-06"]},"7":{"lst":["str",1,"order_month"]},"8":{"rec":{"1":{"str":"iceberg"},"2":{"rec":{"1":{"i32":2},"2":{"i32":1},"3":{"lst":["rec",2,{"1":{"str":"./be/test/exec/test_data/iceberg_reader/00001-0-3cdfaaeb-1a8a-4c75-9f86-61c3bd330d90-00432.parquet"},"4":{"i32":2},"5":{"lst":["i32",4,1,34,2,101]}},{"1":{"str":"./be/test/exec/test_data/iceberg_reader/00001-0-3cdfaaeb-1a8a-4c75-9f86-61c3bd330d90-00961.parquet"},"4":{"i32":2},"5":{"lst":["i32",4,1,34,2,101]}}]}}}}}})|";
    TFileRangeDesc t_scan_range = apache::thrift::from_json_string<doris::TFileRangeDesc>(scan_range_json);

    auto p_reader = new ParquetReader(nullptr, t_scan_param, t_scan_range, 4000, &ctz);
    p_reader->open();

    IcebergTableReader* iceberg_reader = new IcebergTableReader((GenericReader*)p_reader, _profile, &_state,
                                               t_scan_param, t_scan_range, _kv_cache);

    std::vector<std::string> file_col_names{"id", "order_id", "adowner_id", "order_time", "modify_time"};
    std::unordered_map<int, std::string> col_id_name_map{
        {1, "id"}, {2, "order_id"}, {34, "adowner_id"}, {75, "order_time"}, {79, "modify_time"}
    };

    std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
    colname_to_value_range["id"] = ColumnValueRange<TYPE_BIGINT>("id");
    colname_to_value_range["order_id"] = ColumnValueRange<TYPE_BIGINT>("order_id", 258696459711L, 258696459712L, false, -1, -1);
    colname_to_value_range["modify_time"] = ColumnValueRange<TYPE_DATETIME>("modify_time");
    colname_to_value_range["adowner_id"] = ColumnValueRange<TYPE_STRING>("adowner_id");
    colname_to_value_range["order_time"] = ColumnValueRange<TYPE_DATETIME>("order_time");
    ColumnValueRangeType col_range_moth = ColumnValueRange<TYPE_STRING>("order_month");
    const std::string month = "2022-06";
    auto sv_month = StringValue(month.c_str(), month.length());
    ColumnValueRange<TYPE_STRING>::add_fixed_value_range(std::get<7>(col_range_moth), reinterpret_cast<typename PrimitiveTypeTraits<TYPE_STRING>::CppType*>(&sv_month));
    colname_to_value_range["order_month"] = col_range_moth;

    std::string expr_json =
        R"|({"1":{"lst":["rec",11,{"1":{"i32":6},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"3":{"i32":2},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"1":{"str":""},"2":{"str":"and"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"5":{"tf":0},"7":{"str":"and(BOOLEAN, BOOLEAN)"},"11":{"i64":0},"13":{"tf":0}}},"29":{"tf":1}},{"1":{"i32":6},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"3":{"i32":2},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"1":{"str":""},"2":{"str":"and"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"5":{"tf":0},"7":{"str":"and(BOOLEAN, BOOLEAN)"},"11":{"i64":0},"13":{"tf":0}}},"29":{"tf":1}},{"1":{"i32":2},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"3":{"i32":9},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"eq"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"5":{"tf":0},"7":{"str":"eq(TEXT, TEXT)"},"9":{"rec":{"1":{"str":"doris::Operators::eq_string_val_string_val"}}},"11":{"i64":0},"13":{"tf":1}}},"28":{"i32":23},"29":{"tf":1}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":5},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1},"29":{"tf":1}},{"1":{"i32":17},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":15},"2":{"i32":-1}}}}]}}},"4":{"i32":0},"16":{"rec":{"1":{"str":"2022-06"}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":2},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"3":{"i32":14},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"ge"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"5":{"tf":0},"7":{"str":"ge(BIGINT, BIGINT)"},"9":{"rec":{"1":{"str":"doris::Operators::ge_big_int_val_big_int_val"}}},"11":{"i64":0},"13":{"tf":1}}},"28":{"i32":6},"29":{"tf":1}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":1},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1},"29":{"tf":1}},{"1":{"i32":9},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"10":{"rec":{"1":{"i64":258696459711}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":2},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"3":{"i32":12},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"le"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]}}},"5":{"tf":0},"7":{"str":"le(BIGINT, BIGINT)"},"9":{"rec":{"1":{"str":"doris::Operators::le_big_int_val_big_int_val"}}},"11":{"i64":0},"13":{"tf":1}}},"28":{"i32":6},"29":{"tf":1}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":1},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1},"29":{"tf":1}},{"1":{"i32":9},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":0},"10":{"rec":{"1":{"i64":258696459712}}},"20":{"i32":-1},"29":{"tf":0}}]}})|";
    doris::TExpr exprx = apache::thrift::from_json_string<doris::TExpr>(expr_json);
    doris::vectorized::VExprContext* context = nullptr;
    doris::vectorized::VExpr::create_expr_tree(_state.obj_pool(), exprx, &context);
    state = context->prepare(&_state, row_desc);
    ASSERT_TRUE(state.ok());
    state = context->open(&_state);
    ASSERT_TRUE(state.ok());

    state = iceberg_reader->init_reader(file_col_names, col_id_name_map, &colname_to_value_range, context);
    state = iceberg_reader->init_row_filters(t_scan_range);

    auto partition_slot_desc = const_cast<doris::SlotDescriptor*>(desc_tbl->get_slot_descriptor(5));
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    partition_columns["order_month"] = std::make_tuple("2022-06", partition_slot_desc);  
    std::unordered_map<std::string, VExprContext*> missing_columns;
    iceberg_reader->set_fill_columns(partition_columns, missing_columns);


    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    Block* block = new Block();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type =
                vectorized::DataTypeFactory::instance().create_data_type(slot_desc->type(), true);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }

    size_t read_rows = 0;
    bool cur_reader_eof;
    state = iceberg_reader->get_next_block(block, &read_rows, &cur_reader_eof);

    std::cout << block->dump_data();
    for (auto& col : block->get_columns_with_type_and_name()) {
        ASSERT_EQ(col.column->size(), 0);
    }

    context->close(&_state);
    delete block;
    delete p_reader;
}

} // namespace vectorized
} // namespace doris
