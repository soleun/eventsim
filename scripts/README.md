eventsim Config Generator
=========================

This python script generates a configuration file for eventsim by taking event descriptions (nodes) and transitions (edges).

Example input files are included in example folder.

Usage
------

    $ python eventsim_config_generator.py <node file path> <edge file path> <output configuration path>
    
Example
-------
    $ python eventsim_config_generator.py examples/event_map_v3_nodes.csv examplex/event_map_v4_edges.csv eventsim_config.json
