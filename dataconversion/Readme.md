---
title: Converting JSON to flat text
---

The program converts the JSON file to a flat text, with a '|' as the field separator. Each row will be separated by a new line.
The program assumes that each object is one row of JSON.

##To compile and package
Run the following command

```bash
mvn clean package appassembler:assemble
```

##To run

```bash
chmod 755 target/appassembler/bin/JsonToTextUtill
target/appassembler/bin/JsonToTextUtill review input/review.json output/review.txt
target/appassembler/bin/JsonToTextUtill business input/business.json output/business.txt
target/appassembler/bin/JsonToTextUtill user input/user.json output/user.txt
```

