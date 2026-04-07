package com.example.demo.kv;

final class TarantoolLuaScripts {

    static final String PUT = """
            local space = box.space[...]
            local key = select(2, ...)
            local value = select(3, ...)
            space:replace({key, value})
            return true
            """;

    static final String GET = """
            local space = box.space[...]
            local key = select(2, ...)
            local tuple = space:get({key})
            if tuple == nil then
              return nil
            end
            return {tuple[1], tuple[2]}
            """;

    static final String DELETE = """
            local space = box.space[...]
            local key = select(2, ...)
            local tuple = space:delete({key})
            return tuple ~= nil
            """;

    static final String COUNT = """
            local space = box.space[...]
            return space:count()
            """;

    static final String RANGE_PAGE = """
            local space = box.space[...]
            local from_key = select(2, ...)
            local to_key = select(3, ...)
            local exclusive_from = select(4, ...)
            local page_size = select(5, ...)
            local out = {}

            for _, tuple in space.index[0]:pairs({from_key}, {iterator = 'GE'}) do
              local k = tuple[1]
              if exclusive_from and k == from_key then
                goto continue
              end
              if to_key ~= nil and to_key ~= '' and k > to_key then
                break
              end
              table.insert(out, {k, tuple[2]})
              if #out >= page_size then
                break
              end
              ::continue::
            end
            return out
            """;

    private TarantoolLuaScripts() {
    }
}
