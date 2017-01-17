package rh

// Txt describes the interface used for data export.
type Txt interface {
	TxtHeader() string
	TxtValues() string
}
