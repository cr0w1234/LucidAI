VALID_CONTENT_IDENTIFIER_TEMPLATE = """
Determine whether the following text contains meaningful, comprehensible content.  

Input Text:
```{text_input}```

Evaulate Input Text based on following instructions:

If and only the text is:  
- Empty or blank  
- A placeholder or notice that the content needs updating (not complete)  
- Pseudo-Latin or partial Pseudo-Latin 
Do not provide any explanation or context. Return only:
False

Otherwise, do not provide any explanation or context. Return only:
True

Example 1:
```
Input Text:
Faucibus vel sapien lacus quis vitae. Apples are great.. Purus mollis tincidunt lectus vel accumsan cras quisque pellentesque lacinia. Date is 2026.
Output:
False
```
"""