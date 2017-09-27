# Simulator

Market simulator to play back trades from markets concurrently

To run the simulator

1. build the executable with `stack build`
2. run the executable with `stack exec simulator -- market_a.csv market_b.csv market_c.csv`

This will print out totals from all three markets every second
