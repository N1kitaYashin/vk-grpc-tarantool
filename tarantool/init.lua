box.cfg{listen = 3301}

box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, {if_not_exists = true})

local s = box.schema.space.create('KV', {if_not_exists = true})
s:format({
  {name = 'key', type = 'string'},
  {name = 'value', type = 'varbinary', is_nullable = true}
})
s:create_index('primary', {
  type = 'tree',
  parts = {{field = 'key', type = 'string'}},
  if_not_exists = true
})

print('KV space is ready on 3301')
