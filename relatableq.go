package irelate

// relatableQueue implements the heap interface and is used to send Relatables
// back the the caller in order (as deteremined by Less()).
type relatableQueue struct {
	rels []Relatable
	less func(a, b Relatable) bool
}

func (q relatableQueue) Len() int { return len(q.rels) }
func (q relatableQueue) Less(i, j int) bool {
	return q.less(q.rels[i], q.rels[j])
}
func (q relatableQueue) Swap(i, j int) {
	if i < len(q.rels) {
		q.rels[j], q.rels[i] = q.rels[i], q.rels[j]
	}
}
func (q *relatableQueue) Push(i interface{}) {
	iv := i.(Relatable)
	(*q).rels = append((*q).rels, iv)
}

func (q *relatableQueue) Pop() interface{} {
	n := len((*q).rels)
	if n == 0 {
		return nil
	}
	old := (*q).rels
	iv := old[n-1]
	(*q).rels = old[0 : n-1]
	return iv
}
