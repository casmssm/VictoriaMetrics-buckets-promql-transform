package logstorage

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type statsFieldsMin struct {
	srcField string

	fetchFields []string
}

func (sm *statsFieldsMin) String() string {
	s := "fields_min(" + quoteTokenIfNeeded(sm.srcField)
	if len(sm.fetchFields) > 0 {
		s += ", " + fieldNamesString(sm.fetchFields)
	}
	s += ")"
	return s
}

func (sm *statsFieldsMin) updateNeededFields(neededFields fieldsSet) {
	if len(sm.fetchFields) == 0 {
		neededFields.add("*")
	} else {
		neededFields.addFields(sm.fetchFields)
	}
	neededFields.add(sm.srcField)
}

func (sm *statsFieldsMin) newStatsProcessor() (statsProcessor, int) {
	smp := &statsFieldsMinProcessor{
		sm: sm,
	}
	return smp, int(unsafe.Sizeof(*smp))
}

type statsFieldsMinProcessor struct {
	sm *statsFieldsMin

	min string

	fields []Field
}

func (smp *statsFieldsMinProcessor) updateStatsForAllRows(br *blockResult) int {
	stateSizeIncrease := 0

	c := br.getColumnByName(smp.sm.srcField)
	if c.isConst {
		v := c.valuesEncoded[0]
		stateSizeIncrease += smp.updateState(v, br, 0)
		return stateSizeIncrease
	}
	if c.isTime {
		bb := bbPool.Get()
		bb.B = marshalTimestampRFC3339NanoString(bb.B[:0], br.timestamps[0])
		v := bytesutil.ToUnsafeString(bb.B)
		stateSizeIncrease += smp.updateState(v, br, 0)
		bbPool.Put(bb)
		return stateSizeIncrease
	}

	needUpdateState := false
	switch c.valueType {
	case valueTypeString:
		needUpdateState = true
	case valueTypeDict:
		for _, v := range c.dictValues {
			if smp.needUpdateStateString(v) {
				needUpdateState = true
				break
			}
		}
	case valueTypeUint8, valueTypeUint16, valueTypeUint32, valueTypeUint64:
		bb := bbPool.Get()
		bb.B = marshalUint64String(bb.B[:0], c.minValue)
		needUpdateState = smp.needUpdateStateBytes(bb.B)
		bbPool.Put(bb)
	case valueTypeFloat64:
		f := math.Float64frombits(c.minValue)
		bb := bbPool.Get()
		bb.B = marshalFloat64String(bb.B[:0], f)
		needUpdateState = smp.needUpdateStateBytes(bb.B)
		bbPool.Put(bb)
	case valueTypeIPv4:
		bb := bbPool.Get()
		bb.B = marshalIPv4String(bb.B[:0], uint32(c.minValue))
		needUpdateState = smp.needUpdateStateBytes(bb.B)
		bbPool.Put(bb)
	case valueTypeTimestampISO8601:
		bb := bbPool.Get()
		bb.B = marshalTimestampISO8601String(bb.B[:0], int64(c.minValue))
		needUpdateState = smp.needUpdateStateBytes(bb.B)
		bbPool.Put(bb)
	default:
		logger.Panicf("BUG: unknown valueType=%d", c.valueType)
	}

	if needUpdateState {
		values := c.getValues(br)
		for i, v := range values {
			stateSizeIncrease += smp.updateState(v, br, i)
		}
	}

	return stateSizeIncrease
}

func (smp *statsFieldsMinProcessor) updateStatsForRow(br *blockResult, rowIdx int) int {
	stateSizeIncrease := 0

	c := br.getColumnByName(smp.sm.srcField)
	if c.isConst {
		v := c.valuesEncoded[0]
		stateSizeIncrease += smp.updateState(v, br, rowIdx)
		return stateSizeIncrease
	}
	if c.isTime {
		bb := bbPool.Get()
		bb.B = marshalTimestampRFC3339NanoString(bb.B[:0], br.timestamps[rowIdx])
		v := bytesutil.ToUnsafeString(bb.B)
		stateSizeIncrease += smp.updateState(v, br, rowIdx)
		bbPool.Put(bb)
		return stateSizeIncrease
	}

	v := c.getValueAtRow(br, rowIdx)
	stateSizeIncrease += smp.updateState(v, br, rowIdx)

	return stateSizeIncrease
}

func (smp *statsFieldsMinProcessor) mergeState(sfp statsProcessor) {
	src := sfp.(*statsFieldsMinProcessor)
	if smp.needUpdateStateString(src.min) {
		smp.min = src.min
		smp.fields = src.fields
	}
}

func (smp *statsFieldsMinProcessor) needUpdateStateBytes(b []byte) bool {
	v := bytesutil.ToUnsafeString(b)
	return smp.needUpdateStateString(v)
}

func (smp *statsFieldsMinProcessor) needUpdateStateString(v string) bool {
	if v == "" {
		return false
	}
	return smp.min == "" || lessString(v, smp.min)
}

func (smp *statsFieldsMinProcessor) updateState(v string, br *blockResult, rowIdx int) int {
	stateSizeIncrease := 0

	if !smp.needUpdateStateString(v) {
		// There is no need in updating state
		return stateSizeIncrease
	}

	stateSizeIncrease -= len(smp.min)
	stateSizeIncrease += len(v)
	smp.min = strings.Clone(v)

	fields := smp.fields
	for _, f := range fields {
		stateSizeIncrease -= len(f.Name) + len(f.Value)
	}

	clear(fields)
	fields = fields[:0]
	fetchFields := smp.sm.fetchFields
	if len(fetchFields) == 0 {
		cs := br.getColumns()
		for _, c := range cs {
			v := c.getValueAtRow(br, rowIdx)
			fields = append(fields, Field{
				Name:  strings.Clone(c.name),
				Value: strings.Clone(v),
			})
			stateSizeIncrease += len(c.name) + len(v)
		}
	} else {
		for _, field := range fetchFields {
			c := br.getColumnByName(field)
			v := c.getValueAtRow(br, rowIdx)
			fields = append(fields, Field{
				Name:  strings.Clone(c.name),
				Value: strings.Clone(v),
			})
			stateSizeIncrease += len(c.name) + len(v)
		}
	}
	smp.fields = fields

	return stateSizeIncrease
}

func (smp *statsFieldsMinProcessor) finalizeStats() string {
	bb := bbPool.Get()
	bb.B = marshalFieldsToJSON(bb.B, smp.fields)
	result := string(bb.B)
	bbPool.Put(bb)

	return result
}

func parseStatsFieldsMin(lex *lexer) (*statsFieldsMin, error) {
	if !lex.isKeyword("fields_min") {
		return nil, fmt.Errorf("unexpected func; got %q; want 'fields_min'", lex.token)
	}
	lex.nextToken()
	fields, err := parseFieldNamesInParens(lex)
	if err != nil {
		return nil, fmt.Errorf("cannot parse 'fields_min' args: %w", err)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("missing first arg for 'fields_min' func - source field")
	}

	srcField := fields[0]
	fetchFields := fields[1:]
	if slices.Contains(fetchFields, "*") {
		fetchFields = nil
	}

	sm := &statsFieldsMin{
		srcField:    srcField,
		fetchFields: fetchFields,
	}
	return sm, nil
}
