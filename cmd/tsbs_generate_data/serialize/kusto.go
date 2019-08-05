package serialize

import (
	"io"
	"time"
)

// TimescaleDBSerializer writes a Point in a serialized form for TimescaleDB
type CsvSerializer struct{}

// Serialize writes Point p to the given Writer w, so it can be
// loaded by the TimescaleDB loader. The format is CSV with two lines per Point,
// with the first row being the tags and the second row being the field values.
//
// e.g.,
// tags,<tag1>,<tag2>,<tag3>,...
// <measurement>,<timestamp>,<field1>,<field2>,<field3>,...
func (s *CsvSerializer) Serialize(p *Point, w io.Writer) error {
	buf := make([]byte, 0, 256)
	//buf = append(buf, []byte("tags")...)
	//buf = append(buf, []byte(fmt.Sprintf("%d", p.timestamp.UTC().UnixNano()))...)
	buf = append(buf, []byte(p.timestamp.UTC().Format(time.RFC3339))...)
	for i := 0; i < len(p.tagKeys); i++ {
		buf = append(buf, ',')
		buf = append(buf, p.tagValues[i]...)
	}
	//buf = append(buf, '\n')
	_, err := w.Write(buf)
	if err != nil {
		return err
	}

	// Field row second
	buf = make([]byte, 0, 256)
	//buf = append(buf, p.measurementName...)
	//buf = append(buf, ',')

	for i := 0; i < len(p.fieldKeys); i++ {
		buf = append(buf, ',')
		v := p.fieldValues[i]
		buf = fastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err = w.Write(buf)
	return err
}
