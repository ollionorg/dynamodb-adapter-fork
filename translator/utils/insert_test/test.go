package main

import (
	"encoding/json"
	"fmt"

	translator "github.com/cloudspannerecosystem/dynamodb-adapter/translator/utils"
)

func main() {
	transaltorObj := translator.Translator{}
	query := "INSERT INTO employee VALUE {'emp_id': 10, 'first_name': 'Marc', 'last_name': 'Richards1', 'age': 10, 'address': 'Shamli'};"
	res, err := transaltorObj.ToSpannerInsert(query)
	if err != nil {
		fmt.Println(err)
	}
	if err != nil {
		fmt.Println(err)
	}
	a, _ := json.Marshal(res)
	fmt.Println("response-> ", string(a))
}
