package matching

const (
	Red   = true
	Black = false
)

type Order struct {
	ID        string
	Symbol    string
	Side      int
	Price     float64
	Quantity  int64
	Remaining int64
	OrdType   int
	Timestamp int64
	ConnID    string
}

type OrderNode struct {
	Order *Order
	Prev  *OrderNode
	Next  *OrderNode
}

type OrderList struct {
	Head *OrderNode
	Tail *OrderNode
	Size int
}

func NewOrderList() *OrderList {
	return &OrderList{}
}

func (ol *OrderList) Append(order *Order) *OrderNode {
	node := &OrderNode{Order: order}
	if ol.Tail == nil {
		ol.Head = node
		ol.Tail = node
	} else {
		node.Prev = ol.Tail
		ol.Tail.Next = node
		ol.Tail = node
	}
	ol.Size++
	return node
}

func (ol *OrderList) Remove(node *OrderNode) {
	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		ol.Head = node.Next
	}
	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		ol.Tail = node.Prev
	}
	node.Prev = nil
	node.Next = nil
	ol.Size--
}

func (ol *OrderList) PeekFirst() *Order {
	if ol.Head == nil {
		return nil
	}
	return ol.Head.Order
}

func (ol *OrderList) PopFirst() *Order {
	if ol.Head == nil {
		return nil
	}
	node := ol.Head
	ol.Remove(node)
	return node.Order
}

type RBNode struct {
	Price    float64
	Orders   *OrderList
	Color    bool
	Left     *RBNode
	Right    *RBNode
	Parent   *RBNode
	nodeMap  map[string]*OrderNode
}

func newRBNode(price float64) *RBNode {
	return &RBNode{
		Price:   price,
		Orders:  NewOrderList(),
		Color:   Red,
		nodeMap: make(map[string]*OrderNode),
	}
}

func (n *RBNode) addOrder(order *Order) {
	on := n.Orders.Append(order)
	n.nodeMap[order.ID] = on
}

func (n *RBNode) removeOrder(orderID string) {
	on, ok := n.nodeMap[orderID]
	if !ok {
		return
	}
	n.Orders.Remove(on)
	delete(n.nodeMap, orderID)
}

func (n *RBNode) isEmpty() bool {
	return n.Orders.Size == 0
}

type RBTree struct {
	Root      *RBNode
	sentinel  *RBNode
	ascending bool
}

func NewRBTree(ascending bool) *RBTree {
	sentinel := &RBNode{Color: Black}
	return &RBTree{
		sentinel:  sentinel,
		ascending: ascending,
	}
}

func (t *RBTree) compare(a, b float64) int {
	if a < b {
		if t.ascending {
			return -1
		}
		return 1
	}
	if a > b {
		if t.ascending {
			return 1
		}
		return -1
	}
	return 0
}

func (t *RBTree) Insert(price float64, order *Order) {
	node := t.findOrInsert(price)
	node.addOrder(order)
}

func (t *RBTree) findOrInsert(price float64) *RBNode {
	if t.Root == nil {
		t.Root = newRBNode(price)
		t.Root.Color = Black
		t.Root.Left = t.sentinel
		t.Root.Right = t.sentinel
		t.Root.Parent = nil
		return t.Root
	}

	current := t.Root
	var parent *RBNode

	for current != nil && current != t.sentinel {
		parent = current
		cmp := t.compare(price, current.Price)
		if cmp == 0 {
			return current
		} else if cmp < 0 {
			current = current.Left
		} else {
			current = current.Right
		}
	}

	newNode := newRBNode(price)
	newNode.Left = t.sentinel
	newNode.Right = t.sentinel
	newNode.Parent = parent

	cmp := t.compare(price, parent.Price)
	if cmp < 0 {
		parent.Left = newNode
	} else {
		parent.Right = newNode
	}

	t.fixInsert(newNode)
	return newNode
}

func (t *RBTree) fixInsert(node *RBNode) {
	for node.Parent != nil && node.Parent.Color == Red {
		if node.Parent == node.Parent.Parent.Left {
			uncle := node.Parent.Parent.Right
			if uncle != t.sentinel && uncle.Color == Red {
				node.Parent.Color = Black
				uncle.Color = Black
				node.Parent.Parent.Color = Red
				node = node.Parent.Parent
			} else {
				if node == node.Parent.Right {
					node = node.Parent
					t.rotateLeft(node)
				}
				node.Parent.Color = Black
				node.Parent.Parent.Color = Red
				t.rotateRight(node.Parent.Parent)
			}
		} else {
			uncle := node.Parent.Parent.Left
			if uncle != t.sentinel && uncle.Color == Red {
				node.Parent.Color = Black
				uncle.Color = Black
				node.Parent.Parent.Color = Red
				node = node.Parent.Parent
			} else {
				if node == node.Parent.Left {
					node = node.Parent
					t.rotateRight(node)
				}
				node.Parent.Color = Black
				node.Parent.Parent.Color = Red
				t.rotateLeft(node.Parent.Parent)
			}
		}
	}
	t.Root.Color = Black
}

func (t *RBTree) rotateLeft(x *RBNode) {
	y := x.Right
	x.Right = y.Left
	if y.Left != t.sentinel {
		y.Left.Parent = x
	}
	y.Parent = x.Parent
	if x.Parent == nil {
		t.Root = y
	} else if x == x.Parent.Left {
		x.Parent.Left = y
	} else {
		x.Parent.Right = y
	}
	y.Left = x
	x.Parent = y
}

func (t *RBTree) rotateRight(x *RBNode) {
	y := x.Left
	x.Left = y.Right
	if y.Right != t.sentinel {
		y.Right.Parent = x
	}
	y.Parent = x.Parent
	if x.Parent == nil {
		t.Root = y
	} else if x == x.Parent.Right {
		x.Parent.Right = y
	} else {
		x.Parent.Left = y
	}
	y.Right = x
	x.Parent = y
}

func (t *RBTree) Find(price float64) *RBNode {
	current := t.Root
	for current != nil && current != t.sentinel {
		if current.Price == price {
			return current
		}
		cmp := t.compare(price, current.Price)
		if cmp < 0 {
			current = current.Left
		} else {
			current = current.Right
		}
	}
	return nil
}

func (t *RBTree) RemoveOrder(price float64, orderID string) {
	node := t.Find(price)
	if node == nil {
		return
	}
	node.removeOrder(orderID)
	if node.isEmpty() {
		t.deleteNode(node)
	}
}

func (t *RBTree) deleteNode(z *RBNode) {
	var x *RBNode
	var y *RBNode
	yOriginalColor := true

	if z.Left == t.sentinel {
		x = z.Right
		y = z
		yOriginalColor = y.Color
		t.transplant(z, z.Right)
	} else if z.Right == t.sentinel {
		x = z.Left
		y = z
		yOriginalColor = y.Color
		t.transplant(z, z.Left)
	} else {
		y = t.minimum(z.Right)
		yOriginalColor = y.Color
		x = y.Right
		if y.Parent == z {
			if x != t.sentinel {
				x.Parent = y
			}
		} else {
			t.transplant(y, y.Right)
			y.Right = z.Right
			y.Right.Parent = y
		}
		t.transplant(z, y)
		y.Left = z.Left
		y.Left.Parent = y
		y.Color = z.Color
	}

	if !yOriginalColor && x != nil && x != t.sentinel {
		t.fixDelete(x)
	}
}

func (t *RBTree) transplant(u, v *RBNode) {
	if u.Parent == nil {
		t.Root = v
	} else if u == u.Parent.Left {
		u.Parent.Left = v
	} else {
		u.Parent.Right = v
	}
	if v != t.sentinel {
		v.Parent = u.Parent
	}
}

func (t *RBTree) minimum(node *RBNode) *RBNode {
	for node.Left != nil && node.Left != t.sentinel {
		node = node.Left
	}
	return node
}

func (t *RBTree) maximum(node *RBNode) *RBNode {
	for node.Right != nil && node.Right != t.sentinel {
		node = node.Right
	}
	return node
}

func (t *RBTree) fixDelete(x *RBNode) {
	for x != t.Root && x.Color == Black {
		if x == x.Parent.Left {
			w := x.Parent.Right
			if w.Color == Red {
				w.Color = Black
				x.Parent.Color = Red
				t.rotateLeft(x.Parent)
				w = x.Parent.Right
			}
			if w.Left.Color == Black && w.Right.Color == Black {
				w.Color = Red
				x = x.Parent
			} else {
				if w.Right.Color == Black {
					w.Left.Color = Black
					w.Color = Red
					t.rotateRight(w)
					w = x.Parent.Right
				}
				w.Color = x.Parent.Color
				x.Parent.Color = Black
				w.Right.Color = Black
				t.rotateLeft(x.Parent)
				x = t.Root
			}
		} else {
			w := x.Parent.Left
			if w.Color == Red {
				w.Color = Black
				x.Parent.Color = Red
				t.rotateRight(x.Parent)
				w = x.Parent.Left
			}
			if w.Right.Color == Black && w.Left.Color == Black {
				w.Color = Red
				x = x.Parent
			} else {
				if w.Left.Color == Black {
					w.Right.Color = Black
					w.Color = Red
					t.rotateLeft(w)
					w = x.Parent.Left
				}
				w.Color = x.Parent.Color
				x.Parent.Color = Black
				w.Left.Color = Black
				t.rotateRight(x.Parent)
				x = t.Root
			}
		}
	}
	x.Color = Black
}

func (t *RBTree) BestPrice() (float64, bool) {
	if t.Root == nil {
		return 0, false
	}
	node := t.minimum(t.Root)
	return node.Price, true
}

func (t *RBTree) BestOrders() *OrderList {
	if t.Root == nil {
		return nil
	}
	node := t.minimum(t.Root)
	return node.Orders
}

func (t *RBTree) PopBestOrder() *Order {
	if t.Root == nil {
		return nil
	}
	node := t.minimum(t.Root)
	order := node.Orders.PopFirst()
	delete(node.nodeMap, order.ID)
	if node.isEmpty() {
		t.deleteNode(node)
	}
	return order
}

func (t *RBTree) IsEmpty() bool {
	return t.Root == nil
}

func (t *RBTree) CountLevels() int {
	return t.countLevels(t.Root)
}

func (t *RBTree) countLevels(node *RBNode) int {
	if node == nil || node == t.sentinel {
		return 0
	}
	left := t.countLevels(node.Left)
	right := t.countLevels(node.Right)
	if left > right {
		return left + 1
	}
	return right + 1
}
