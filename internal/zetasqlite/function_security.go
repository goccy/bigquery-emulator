package zetasqlite

func SESSION_USER() (Value, error) {
	return StringValue("dummy"), nil
}
