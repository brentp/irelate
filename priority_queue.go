/*
Copyright 2014 Workiva, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
The priority queue is almost a spitting image of the logic
used for a regular queue.  In order to keep the logic fast,
this code is repeated instead of using casts to cast to interface{}
back and forth.  If Go had inheritance and generics, this problem
would be easier to solve.
*/

package irelate

import "github.com/brentp/irelate/interfaces"

type priorityItems []interfaces.Relatable

func (items *priorityItems) swap(i, j int) {
	(*items)[i], (*items)[j] = (*items)[j], (*items)[i]
}

func (items *priorityItems) pop(less func(a, b interfaces.Relatable) bool) interfaces.Relatable {
	size := len(*items)

	// Move last leaf to root, and 'pop' the last item.
	items.swap(size-1, 0)
	item := (*items)[size-1] // Item to return.
	(*items)[size-1], *items = nil, (*items)[:size-1]

	// 'Bubble down' to restore heap property.
	index := 0
	childL, childR := 2*index+1, 2*index+2
	for len(*items) > childL {
		child := childL
		if len(*items) > childR && less((*items)[childR], (*items)[childL]) {
			child = childR
		}

		if less((*items)[child], (*items)[index]) {
			items.swap(index, child)

			index = child
			childL, childR = 2*index+1, 2*index+2
		} else {
			break
		}
	}

	return item
}

func (items *priorityItems) get(number int, less func(a, b interfaces.Relatable) bool) []interfaces.Relatable {
	returnItems := make([]interfaces.Relatable, 0, number)
	for i := 0; i < number; i++ {
		if i >= len(*items) {
			break
		}

		returnItems = append(returnItems, items.pop(less))
	}

	return returnItems
}

func (items *priorityItems) push(item interfaces.Relatable, less func(a, b interfaces.Relatable) bool) {
	// Stick the item as the end of the last level.
	*items = append(*items, item)

	// 'Bubble up' to restore heap property.
	index := len(*items) - 1
	parent := int((index - 1) / 2)
	for parent >= 0 && !less((*items)[parent], item) {
		items.swap(index, parent)

		index = parent
		parent = int((index - 1) / 2)
	}
}

// PriorityQueue is similar to queue except that it takes
// items that implement the Item interface and adds them
// to the queue in priority order.
type PriorityQueue struct {
	less            func(a, b interfaces.Relatable) bool
	items           priorityItems
	itemMap         map[interfaces.Relatable]struct{}
	allowDuplicates bool
}

// Put adds items to the queue.
func (pq *PriorityQueue) Put(items ...interfaces.Relatable) error {
	if len(items) == 0 {
		return nil
	}

	for _, item := range items {
		if pq.allowDuplicates {
			pq.items.push(item, pq.less)
		} else if _, ok := pq.itemMap[item]; !ok {
			pq.itemMap[item] = struct{}{}
			pq.items.push(item, pq.less)
		}
	}

	return nil
}

// Get retrieves items from the queue.  If the queue is empty,
// this call blocks until the next item is added to the queue.  This
// will attempt to retrieve number of items.
func (pq *PriorityQueue) Get(number int) ([]interfaces.Relatable, error) {
	if number < 1 {
		return nil, nil
	}

	var items []interfaces.Relatable

	// Remove references to popped items.
	deleteItems := func(items []interfaces.Relatable) {
		for _, item := range items {
			delete(pq.itemMap, item)
		}
	}

	if len(pq.items) == 0 {
		items = pq.items.get(number, pq.less)
		if !pq.allowDuplicates {
			deleteItems(items)
		}
		return items, nil
	}

	items = pq.items.get(number, pq.less)
	deleteItems(items)
	return items, nil
}

// Peek will look at the next item without removing it from the queue.
func (pq *PriorityQueue) Peek() interfaces.Relatable {
	if len(pq.items) > 0 {
		return pq.items[0]
	}
	return nil
}

// Empty returns a bool indicating if there are any items left
// in the queue.
func (pq *PriorityQueue) Empty() bool {

	return len(pq.items) == 0
}

// Len returns a number indicating how many items are in the queue.
func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

// NewPriorityQueue is the constructor for a priority queue.
func NewPriorityQueue(hint int, allowDuplicates bool, less func(a, b interfaces.Relatable) bool) *PriorityQueue {
	return &PriorityQueue{
		less:            less,
		items:           make(priorityItems, 0, hint),
		itemMap:         make(map[interfaces.Relatable]struct{}, hint),
		allowDuplicates: allowDuplicates,
	}
}
