package server

type Storage string

const (
	MemoryStorage Storage = "file::memory:?cache=shared"
	TempStorage   Storage = "tmp"
)
