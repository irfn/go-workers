package wrkrs

type WorkersLogger interface {
	Println(...interface{})
	Printf(string, ...interface{})
}
