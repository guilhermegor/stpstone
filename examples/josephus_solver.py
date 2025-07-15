from stpstone.analytics.quant.josephus_solver import JosephusSolver


solver = JosephusSolver(["Alice", "Bob", "Charlie", "Dave", "Eve"], 3)
survivor = solver.solve
print(f"The last survivor is: {survivor}")
