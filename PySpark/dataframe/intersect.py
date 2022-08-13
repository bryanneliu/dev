overlap = qu.join(candidates, (candidates.src == qu.src) & (candidates.dest == qu.dest))

overlap = candidates.intersect(qu)