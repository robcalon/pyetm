import pandas as pd

curves = pd.DataFrame(
    data = {
        "category_a.input (MW)": [40, 35, 21, 42, 34],
        "category_b.input (MW)": [69, 52, 45, 64, 95],
        "category_c.input (MW)": [0, 0, 0, 2, 1],
        "category_d.input (MW)": [38, 35, 24, 65, 36],
        "category_a.output (MW)": [95, 67, 0, 75, 0],
        "category_b.output (MW)": [19, 50, 0, 63, 0],
        "category_c.output (MW)": [33, 5, 90, 35, 0],
        "deficit": [0, 0, 0, 0, 166],
    }
)

mapping = pd.Series(
    data = {
        "category_a.input (MW)": "mapping_a",
        "category_b.input (MW)": "mapping_a",
        "category_c.input (MW)": "mapping_b",
        "category_d.input (MW)": "mapping_b",
        "category_a.output (MW)": "mapping_a",
        "category_b.output (MW)": "mapping_a",
        "category_c.output (MW)": "mapping_b",
        "deficit": "deficit",
    }, 
    name="user_keys"
)

reg = pd.DataFrame(
    data = {
        "category_a.input (MW)": {
            "node_a": 1.00, 
            "node_b": 0.00,
            "node_c": 0.00,
            "node_d": 0.00,
            "node_e": 0.00,
        },

        "category_b.input (MW)": {
            "node_a": 0.00, 
            "node_b": 1.00,
            "node_c": 0.00,
            "node_d": 0.00,
            "node_e": 0.00,
        },

        "category_c.input (MW)": {
            "node_a": 0.00, 
            "node_b": 0.00,
            "node_c": 1.00,
            "node_d": 0.00,
            "node_e": 0.00,
        },

        "category_d.input (MW)": {
            "node_a": 0.00, 
            "node_b": 0.00,
            "node_c": 0.00,
            "node_d": 1.00,
            "node_e": 0.00,
        },

        "category_a.output (MW)": {
            "node_a": 1.00, 
            "node_b": 0.00,
            "node_c": 0.00,
            "node_d": 0.00,
            "node_e": 0.00,
        },

        "category_b.output (MW)": {
            "node_a": 0.00, 
            "node_b": 1.00,
            "node_c": 0.00,
            "node_d": 0.00,
            "node_e": 0.00,
        },

        "category_c.output (MW)": {
            "node_a": 0.00, 
            "node_b": 0.00,
            "node_c": 1.00,
            "node_d": 0.00,
            "node_e": 0.00,
        },

        "deficit": {
            "node_a": 0.00, 
            "node_b": 0.00,
            "node_c": 0.00,
            "node_d": 0.00,
            "node_e": 1.00,        
        },
    }
)