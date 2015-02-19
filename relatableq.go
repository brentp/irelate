package irelate

// relatableQueue implements the heap interface and is used to send Relatables
// back the the caller in order (as deteremined by Relatable.Less()).
type relatableQueue []IRelatable

func (q relatableQueue) Len() int { return len(q) }
func (q relatableQueue) Less(i, j int) bool {
	return q[i].Less(q[j])
}
func (q relatableQueue) Swap(i, j int) {
	if i < len(q) {
		q[j], q[i] = q[i], q[j]
	}
}
func (q *relatableQueue) Push(i interface{}) {
	iv := i.(IRelatable)
	*q = append(*q, iv)
}

func (q *relatableQueue) Pop() interface{} {
	n := len(*q)
	if n == 0 {
		return nil
	}
	old := *q
	iv := old[n-1]
	*q = old[0 : n-1]
	return iv
}
