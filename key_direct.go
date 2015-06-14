package astore

// Implements the appendableKey interface for writes that go directly to the keystore
type directKey struct {
	path string
}

func newDirectKey(basepath string) (appendableKey, error) {
	return &directKey{path: basepath}, nil
}

func (kd *directKey) Append(key hashableKey, value []byte) error {
	k, err := OpenKey(kd.path, key)
	if err != nil {
		return err
	}
	return k.Append(value)
}
