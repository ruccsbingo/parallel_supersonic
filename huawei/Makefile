TESTS = parallel_supersonic
#TESTS = query1 query3 query12 query14 querymerge demo

CC = g++
PROTOC = protoc

CFLAGS = -Wall -mmmx -msse -msse2 -O0 -g -DNDEBUG

INCLUDES = -I../ -I../gmock/include/ -I../gmock/gtest/include/

LDFLAGS = -lglog -lgflags -lprotobuf -lre2 -lboost_timer -lboost_system

HEADERS = \
    ../supersonic/base/exception/exception.h \
    ../supersonic/base/exception/exception_macros.h \
    ../supersonic/base/exception/result.h \
    ../supersonic/base/infrastructure/aggregation_operators.h \
    ../supersonic/base/infrastructure/bit_pointers.h \
    ../supersonic/base/infrastructure/block.h \
    ../supersonic/base/infrastructure/copy_column.h \
    ../supersonic/base/infrastructure/double_buffered_block.h \
    ../supersonic/base/infrastructure/init.h \
    ../supersonic/base/infrastructure/operators.h \
    ../supersonic/base/infrastructure/projector.h \
    ../supersonic/base/infrastructure/tuple_schema.h \
    ../supersonic/base/infrastructure/types.h \
    ../supersonic/base/infrastructure/types_infrastructure.h \
    ../supersonic/base/infrastructure/variant.h \
    ../supersonic/base/infrastructure/variant_pointer.h \
    ../supersonic/base/infrastructure/view_copier.h \
    ../supersonic/base/memory/arena.h \
    ../supersonic/base/memory/memory.h \
    ../supersonic/base/memory/memory_mocks.h \
    ../supersonic/cursor/base/cursor.h \
    ../supersonic/cursor/base/cursor_mock.h \
    ../supersonic/cursor/base/cursor_transformer.h \
    ../supersonic/cursor/base/lookup_index.h \
    ../supersonic/cursor/base/operation.h \
    ../supersonic/cursor/core/aggregate.h \
    ../supersonic/cursor/core/aggregator.h \
    ../supersonic/cursor/core/benchmarks.h \
    ../supersonic/cursor/core/coalesce.h \
    ../supersonic/cursor/core/column_aggregator.h \
    ../supersonic/cursor/core/compute.h \
    ../supersonic/cursor/core/filter.h \
    ../supersonic/cursor/core/foreign_filter.h \
    ../supersonic/cursor/core/generate.h \
    ../supersonic/cursor/core/hash_join.h \
    ../supersonic/cursor/core/hybrid_group_utils.h \
    ../supersonic/cursor/core/limit.h \
    ../supersonic/cursor/core/merge_union_all.h \
    ../supersonic/cursor/core/ownership_taker.h \
    ../supersonic/cursor/core/project.h \
    ../supersonic/cursor/core/rowid_merge_join.h \
    ../supersonic/cursor/core/scan_view.h \
    ../supersonic/cursor/core/sort.h \
    ../supersonic/cursor/core/specification_builder.h \
    ../supersonic/cursor/core/splitter.h \
    ../supersonic/cursor/core/spy.h \
    ../supersonic/cursor/infrastructure/basic_cursor.h \
    ../supersonic/cursor/infrastructure/basic_operation.h \
    ../supersonic/cursor/infrastructure/file_io-internal.h \
    ../supersonic/cursor/infrastructure/file_io.h \
    ../supersonic/cursor/infrastructure/history_transformer.h \
    ../supersonic/cursor/infrastructure/iterators.h \
    ../supersonic/cursor/infrastructure/ordering.h \
    ../supersonic/cursor/infrastructure/ownership_revoker.h \
    ../supersonic/cursor/infrastructure/row.h \
    ../supersonic/cursor/infrastructure/row_copier.h \
    ../supersonic/cursor/infrastructure/row_hash_set.h \
    ../supersonic/cursor/infrastructure/table.h \
    ../supersonic/cursor/infrastructure/value_ref.h \
    ../supersonic/cursor/infrastructure/view_cursor.h \
    ../supersonic/cursor/infrastructure/view_printer.h \
    ../supersonic/cursor/infrastructure/writer.h \
    ../supersonic/expression/base/expression.h \
    ../supersonic/expression/core/arithmetic_bound_expressions.h \
    ../supersonic/expression/core/arithmetic_expressions.h \
    ../supersonic/expression/core/comparison_bound_expressions.h \
    ../supersonic/expression/core/comparison_expressions.h \
    ../supersonic/expression/core/date_bound_expressions.h \
    ../supersonic/expression/core/date_evaluators.h \
    ../supersonic/expression/core/date_expressions.h \
    ../supersonic/expression/core/elementary_bound_expressions.h \
    ../supersonic/expression/core/elementary_expressions.h \
    ../supersonic/expression/core/math_bound_expressions.h \
    ../supersonic/expression/core/math_evaluators.h \
    ../supersonic/expression/core/math_expressions.h \
    ../supersonic/expression/core/projecting_bound_expressions.h \
    ../supersonic/expression/core/projecting_expressions.h \
    ../supersonic/expression/core/stateful_bound_expressions.h \
    ../supersonic/expression/core/stateful_expressions.h \
    ../supersonic/expression/core/string_bound_expressions.h \
    ../supersonic/expression/core/string_bound_expressions_internal.h \
    ../supersonic/expression/core/string_evaluators.h \
    ../supersonic/expression/core/string_expressions.h \
    ../supersonic/expression/ext/hashing/hashing_bound_expressions.h \
    ../supersonic/expression/ext/hashing/hashing_evaluators.h \
    ../supersonic/expression/ext/hashing/hashing_expressions.h \
    ../supersonic/expression/infrastructure/basic_bound_expression.h \
    ../supersonic/expression/infrastructure/basic_expressions.h \
    ../supersonic/expression/infrastructure/bound_expression_creators.h \
    ../supersonic/expression/infrastructure/elementary_bound_const_expressions.h \
    ../supersonic/expression/infrastructure/elementary_const_expressions.h \
    ../supersonic/expression/infrastructure/expression_utils.h \
    ../supersonic/expression/infrastructure/terminal_bound_expressions.h \
    ../supersonic/expression/infrastructure/terminal_expressions.h \
    ../supersonic/expression/templated/abstract_bound_expressions.h \
    ../supersonic/expression/templated/abstract_expressions.h \
    ../supersonic/expression/templated/bound_expression_factory.h \
    ../supersonic/expression/templated/cast_bound_expression.h \
    ../supersonic/expression/templated/cast_expression.h \
    ../supersonic/expression/vector/binary_column_computers.h \
    ../supersonic/expression/vector/column_validity_checkers.h \
    ../supersonic/expression/vector/expression_evaluators.h \
    ../supersonic/expression/vector/expression_traits.h \
    ../supersonic/expression/vector/simd_operators.h \
    ../supersonic/expression/vector/ternary_column_computers.h \
    ../supersonic/expression/vector/unary_column_computers.h \
    ../supersonic/expression/vector/vector_logic.h \
    ../supersonic/expression/vector/vector_primitives.h \
    ../supersonic/serialization/build_expression_from_proto.h \
    ../supersonic/supersonic.h \
    ../supersonic/utils/algorithm.h \
    ../supersonic/utils/arm_instruction_set_select.h \
    ../supersonic/utils/atomic/atomic.h \
    ../supersonic/utils/atomic/atomic_internal.h \
    ../supersonic/utils/atomic/atomic_std.h \
    ../supersonic/utils/atomic/forward.h \
    ../supersonic/utils/atomic/locking.h \
    ../supersonic/utils/atomic/tsan.h \
    ../supersonic/utils/atomic/x86-gcc.h \
    ../supersonic/utils/atomic_refcount.h \
    ../supersonic/utils/atomicops-internals-powerpc.h \
    ../supersonic/utils/atomicops-internals-x86.h \
    ../supersonic/utils/atomicops.h \
    ../supersonic/utils/auxiliary/atomicops-internals-arm-generic.h \
    ../supersonic/utils/auxiliary/atomicops-internals-arm-v6plus.h \
    ../supersonic/utils/auxiliary/atomicops-internals-macosx.h \
    ../supersonic/utils/auxiliary/atomicops-internals-windows.h \
    ../supersonic/utils/basictypes.h \
    ../supersonic/utils/bits.h \
    ../supersonic/utils/casts.h \
    ../supersonic/utils/charmap.h \
    ../supersonic/utils/container_literal.h \
    ../supersonic/utils/container_literal_generated.h \
    ../supersonic/utils/cycleclock-inl.h \
    ../supersonic/utils/demangle.h \
    ../supersonic/utils/dynamic_annotations.h \
    ../supersonic/utils/endian.h \
    ../supersonic/utils/exception/coowned_pointer.h \
    ../supersonic/utils/exception/failureor.h \
    ../supersonic/utils/exception/stack_trace.h \
    ../supersonic/utils/file.h \
    ../supersonic/utils/file_util.h \
    ../supersonic/utils/fixedarray.h \
    ../supersonic/utils/hash/hash.h \
    ../supersonic/utils/int128.h \
    ../supersonic/utils/integral_types.h \
    ../supersonic/utils/iterator_adaptors.h \
    ../supersonic/utils/linked_ptr.h \
    ../supersonic/utils/linux_syscall_support.h \
    ../supersonic/utils/logging-inl.h \
    ../supersonic/utils/macros.h \
    ../supersonic/utils/manual_constructor.h \
    ../supersonic/utils/map-util.h \
    ../supersonic/utils/mathlimits.h \
    ../supersonic/utils/mutex.h \
    ../supersonic/utils/once.h \
    ../supersonic/utils/paranoid.h \
    ../supersonic/utils/pointer_vector.h \
    ../supersonic/utils/port.h \
    ../supersonic/utils/random.h \
    ../supersonic/utils/scoped_ptr.h \
    ../supersonic/utils/scoped_ptr_internals.h \
    ../supersonic/utils/shared_ptr.h \
    ../supersonic/utils/singleton.h \
    ../supersonic/utils/sparsetable.h \
    ../supersonic/utils/spinlock_linux-inl.h \
    ../supersonic/utils/spinlock_posix-inl.h \
    ../supersonic/utils/spinlock_wait.h \
    ../supersonic/utils/spinlock_win32-inl.h \
    ../supersonic/utils/stl_util.h \
    ../supersonic/utils/stringprintf.h \
    ../supersonic/utils/strings/ascii_ctype.h \
    ../supersonic/utils/strings/charset.h \
    ../supersonic/utils/strings/escaping.h \
    ../supersonic/utils/strings/fastmem.h \
    ../supersonic/utils/strings/human_readable.h \
    ../supersonic/utils/strings/join.h \
    ../supersonic/utils/strings/memutil.h \
    ../supersonic/utils/strings/numbers.h \
    ../supersonic/utils/strings/serialize.h \
    ../supersonic/utils/strings/split.h \
    ../supersonic/utils/strings/split_internal.h \
    ../supersonic/utils/strings/strcat.h \
    ../supersonic/utils/strings/stringpiece.h \
    ../supersonic/utils/strings/strip.h \
    ../supersonic/utils/strings/substitute.h \
    ../supersonic/utils/strings/util.h \
    ../supersonic/utils/strtoint.h \
    ../supersonic/utils/template_util.h \
    ../supersonic/utils/thread_annotations.h \
    ../supersonic/utils/timer.h \
    ../supersonic/utils/type_traits.h \
    ../supersonic/utils/utf/utf.h \
    ../supersonic/utils/utf/utfdef.h \
    ../supersonic/utils/valgrind.h \
    ../supersonic/utils/walltime.h

OBJECTS = \
    ../supersonic/base/infrastructure/bit_pointers.o \
    ../supersonic/base/infrastructure/block.o \
    ../supersonic/base/infrastructure/copy_column.o \
    ../supersonic/base/infrastructure/double_buffered_block.o \
    ../supersonic/base/infrastructure/projector.o \
    ../supersonic/base/infrastructure/tuple_schema.o \
    ../supersonic/base/infrastructure/types.o \
    ../supersonic/base/infrastructure/types_infrastructure.o \
    ../supersonic/base/infrastructure/view_copier.o \
    ../supersonic/base/memory/arena.o \
    ../supersonic/base/memory/memory.o \
    ../supersonic/base/memory/memory_mocks.o \
    ../supersonic/cursor/base/cursor.o \
    ../supersonic/cursor/base/lookup_index.o \
    ../supersonic/cursor/core/aggregate_clusters.o \
    ../supersonic/cursor/core/aggregate_groups.o \
    ../supersonic/cursor/core/aggregate_scalar.o \
    ../supersonic/cursor/core/aggregator.o \
    ../supersonic/cursor/core/benchmarks.o \
    ../supersonic/cursor/core/coalesce.o \
    ../supersonic/cursor/core/column_aggregator.o \
    ../supersonic/cursor/core/compute.o \
    ../supersonic/cursor/core/filter.o \
    ../supersonic/cursor/core/foreign_filter.o \
    ../supersonic/cursor/core/generate.o \
    ../supersonic/cursor/core/hash_join.o \
    ../supersonic/cursor/core/hybrid_group_utils.o \
    ../supersonic/cursor/core/limit.o \
    ../supersonic/cursor/core/merge_union_all.o \
    ../supersonic/cursor/core/project.o \
    ../supersonic/cursor/core/rowid_merge_join.o \
    ../supersonic/cursor/core/scan_view.o \
    ../supersonic/cursor/core/sort.o \
    ../supersonic/cursor/core/specification_builder.o \
    ../supersonic/cursor/core/splitter.o \
    ../supersonic/cursor/core/spy.o \
    ../supersonic/cursor/infrastructure/basic_cursor.o \
    ../supersonic/cursor/infrastructure/basic_operation.o \
    ../supersonic/cursor/infrastructure/file_io.o \
    ../supersonic/cursor/infrastructure/iterators.o \
    ../supersonic/cursor/infrastructure/ordering.o \
    ../supersonic/cursor/infrastructure/row_hash_set.o \
    ../supersonic/cursor/infrastructure/table.o \
    ../supersonic/cursor/infrastructure/view_cursor.o \
    ../supersonic/cursor/infrastructure/view_printer.o \
    ../supersonic/cursor/infrastructure/writer.o \
    ../supersonic/expression/base/expression.o \
    ../supersonic/expression/core/arithmetic_bound_expressions.o \
    ../supersonic/expression/core/arithmetic_expressions.o \
    ../supersonic/expression/core/comparison_bound_expressions.o \
    ../supersonic/expression/core/comparison_expressions.o \
    ../supersonic/expression/core/date_bound_expressions.o \
    ../supersonic/expression/core/date_evaluators.o \
    ../supersonic/expression/core/date_expressions.o \
    ../supersonic/expression/core/elementary_bound_expressions.o \
    ../supersonic/expression/core/elementary_expressions.o \
    ../supersonic/expression/core/math_bound_expressions.o \
    ../supersonic/expression/core/math_expressions.o \
    ../supersonic/expression/core/projecting_bound_expressions.o \
    ../supersonic/expression/core/projecting_expressions.o \
    ../supersonic/expression/core/stateful_bound_expressions.o \
    ../supersonic/expression/core/stateful_expressions.o \
    ../supersonic/expression/core/string_bound_expressions.o \
    ../supersonic/expression/core/string_expressions.o \
    ../supersonic/expression/ext/hashing/hashing_bound_expressions.o \
    ../supersonic/expression/ext/hashing/hashing_expressions.o \
    ../supersonic/expression/infrastructure/basic_bound_expression.o \
    ../supersonic/expression/infrastructure/basic_expressions.o \
    ../supersonic/expression/infrastructure/expression_utils.o \
    ../supersonic/expression/infrastructure/terminal_bound_expressions.o \
    ../supersonic/expression/infrastructure/terminal_expressions.o \
    ../supersonic/expression/templated/bound_expression_factory.o \
    ../supersonic/expression/templated/cast_bound_expression.o \
    ../supersonic/expression/templated/cast_expression.o \
    ../supersonic/expression/vector/vector_logic.o \
    ../supersonic/serialization/build_expression_from_proto.o \
    ../supersonic/utils/atomic/locking.o \
    ../supersonic/utils/atomic/x86-gcc.o \
    ../supersonic/utils/atomicops-internals-x86.o \
    ../supersonic/utils/bits.o \
    ../supersonic/utils/demangle.o \
    ../supersonic/utils/exception/stack_trace.o \
    ../supersonic/utils/file.o \
    ../supersonic/utils/file_util.o \
    ../supersonic/utils/hash/hash.o \
    ../supersonic/utils/int128.o \
    ../supersonic/utils/mathlimits.o \
    ../supersonic/utils/once.o \
    ../supersonic/utils/random.o \
    ../supersonic/utils/spinlock_wait.o \
    ../supersonic/utils/stringprintf.o \
    ../supersonic/utils/strings/ascii_ctype.o \
    ../supersonic/utils/strings/charset.o \
    ../supersonic/utils/strings/escaping.o \
    ../supersonic/utils/strings/human_readable.o \
    ../supersonic/utils/strings/join.o \
    ../supersonic/utils/strings/memutil.o \
    ../supersonic/utils/strings/numbers.o \
    ../supersonic/utils/strings/serialize.o \
    ../supersonic/utils/strings/split.o \
    ../supersonic/utils/strings/strcat.o \
    ../supersonic/utils/strings/stringpiece.o \
    ../supersonic/utils/strings/strip.o \
    ../supersonic/utils/strings/substitute.o \
    ../supersonic/utils/strings/util.o \
    ../supersonic/utils/strtoint.o \
    ../supersonic/utils/walltime.o

OBJECTS2 = \
    ../supersonic/utils/utf/rune.o

PROTOS = \
    ../supersonic/benchmark/proto/benchmark.proto \
    ../supersonic/cursor/proto/cursors.proto \
    ../supersonic/expression/proto/operators.proto \
    ../supersonic/proto/specification.proto \
    ../supersonic/proto/supersonic.proto \
    ../supersonic/utils/exception/stack_trace.proto \
    ../supersonic/utils/proto/expressions.proto \
    ../supersonic/utils/proto/types.proto

.SECONDEXPANSION:
all: $(TESTS)

$(TESTS): $(OBJECTS) $(OBJECTS2) $(PROTOS:.proto=.pb.o) $$@.o
	$(CC) -o $@ $^ $(LDFLAGS)

$(TESTS:=.o): $$(patsubst %.o, %.cc, $$@) $(PROTOS:.proto=.pb.h) $(HEADERS)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ -c $<

$(OBJECTS): $$(patsubst %.o, %.cc, $$@) $(PROTOS:.proto=.pb.h) $(HEADERS)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ -c $<

$(OBJECTS2): $$(patsubst %.o, %.c, $$@) $(PROTOS:.proto=.pb.h) $(HEADERS)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ -c $<

$(PROTOS:.proto=.pb.o): $$(patsubst %.o, %.cc, $$@) $(PROTOS:.proto=.pb.h) $(HEADERS)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ -c $<

$(PROTOS:.proto=.pb.h): $$(patsubst %.pb.h, %.proto, $$@)
	$(PROTOC) --proto_path=.. --cpp_out=.. $<

$(PROTOS:.proto=.pb.cc): $$(patsubst %.cc, %.h, $$@)

clean:
	-rm -fr $(TESTS) $(TESTS:=.o) $(OBJECTS) $(OBJECTS2) $(PROTOS:.proto=.pb.o) $(PROTOS:.proto=.pb.cc) $(PROTOS:.proto=.pb.h)
