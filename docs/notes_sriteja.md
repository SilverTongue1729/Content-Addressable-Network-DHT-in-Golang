## Notes

- @Keval Join and Takeover node is not part of fault tolerance, its part of load balancing
  - cos load balancing basically needs to ensure that the size of a zone for each node doesn't get too big (or get too many requests). And this would mainly need to be handled when a node crashes and when a crashed node joins.

- dht supports storing unencrypted data and also encrypted data with multiple different keys.

## Questions

- all data for each node is stored in memory, that should be fine for a prototype?
  - arnav's team is storing in memory for now, but might store in disk
  
- keys are prefixed with username, but currently this won't work with decentralised access, since each access point will store its own version of users, 
  - how will each access to each node work, does access to each node itself handle security?? as in operations to a node (including join??) will be denied without auth, so two things:
  1. how to we prevent random nodes from joining the network?
  2. how do we prevent random people from performing operations on the network?

- what happens if a node fails in the process of sending a request?? how is this case handled??

## To fix

**Join and load balance:**
- `shouldTakeOverZone` in `service.go` called by `handleDeadNode` called by `checkDeadNodes`, only checks if this node is closest to the dead zone among its this nodes neighbours which doesn't cover all neighbours of the deadzone which will call `checkDeadNodes`.
- for joining, fix/verify `TakeoverNode` in `replication.go`
- from Claude:
> This implementation is a simplification of zone takeover. In a more sophisticated implementation, there would be:
> - A consensus protocol to decide which neighbor should take over the zone
> - Potential splitting of the failed zone among multiple neighbors
> - More complex zone geometry handling to avoid long, thin zones that could affect routing efficiency.
>
> Currently, this method appears to make a single node take over the entire zone of a failed node, which could lead to load imbalance if not managed carefully. It also assumes that the calling node is the one that should take over, rather than determining the best candidate among all neighbors.


## Load balancing TODO

- Implement and integrate functions in `loadbalancer.go`.
- fix and implement `TakeoverNode` in `replication.go` and `Join` in `service.go`.