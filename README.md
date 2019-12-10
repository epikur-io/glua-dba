# Simple database access for gopher-lua

## Example

```lua
	local dba = require("dba")

	local db = dba.new()
	db:connect("mysql", "user:password@tcp4(127.0.0.1:3306)/mydb?charset=utf8")
	local myQuery = db:query("select * from customers where customerNumber > {:customerNumber} limit {:limit};")
	myQuery:bind({
		customerNumber = "100",
		limit = "50",
	})
	local result = myQuery:run()
	
	db:close()

```

