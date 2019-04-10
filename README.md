# cabbytools
A library for working with MapReduce frameworks, specifically an ahd-hoc fst-file database

# Purpose
Cabbytools allows the user to load pre-prepared text statements that act as pipelines for feeding fst files from directories where they are stored, in essence acting as an autocompletion tool for data scientists to quickly start accessing data. Currently the analytics unit copies data from the TPEP server into the shared drive and hosts a MapReduce style database made up of fst files that represent one day of trip records for each type of industry.

The user is able to access that data and run code rapidly using dplyr or data.table to run quick counts on a custom numnber of days, months or weeks. In addition to quick access, parallelization features are added to allow the user to run jobs in parallel across these different files in a custom manner and cut down reporting speed by 4X or more.

# How to use
Currently the main two functions to leverage are ```get_trips_days```
