package utils

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
)

func MD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

// Array2obj is a helper function to process the first element of raw
func Array2obj(raw interface{}) map[string]interface{} {
	obj := make(map[string]interface{})
	if rawArray, ok := raw.([]interface{}); ok {
		for i, value := range rawArray {
			key := fmt.Sprintf("key%d", i)
			obj[key] = value
		}
	}
	return obj
}

func ConvertToMapString(input map[string]interface{}) (map[string]string, error) {
	output := make(map[string]string)
	for key, value := range input {
		strValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("value for key '%s' is not a string", key)
		}
		output[key] = strValue
	}
	return output, nil
}
