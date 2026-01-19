NER_KEYWORD_EXTRACT_TEMPLATE = """
    You are now an entity recognition model.

    Instructions: An entity is a person, title, named organization, location, country or miscellaneous. Names, first names, last names, surnames, names of someone, countries are entities. Nationalities are entities even if they are adjectives. Sports, sporting events, adjectives, verbs, numbers, adverbs, abstract concepts, sports, are not entities. Dates, years and times are not entities.

    Example 1:
    Input: 
    What is the name of the most famous speech by MLK? 
    Output: 
    MLK, Martin Luther King, Martin Luther King, Jr.

    Example 2:
    Input: 
    Who is the ED of CSHA? 
    Output: 
    ED of CSHA
    
    Given the input below, identify the possible entitities based on instructions, expand the entities if they are known acronyms, and output each entity as a comma-separated list.

    Input:
    ```{query}```
"""