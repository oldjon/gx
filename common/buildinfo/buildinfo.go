package buildinfo

// code version
var codeVersion = ""

// resource version
var resVersion = ""

// build time
var dateTime = ""

// go version
var goVersion = ""

func GetCodeVersion() string {
	return codeVersion
}

func GetResVersion() string {
	return resVersion
}

func GetDateTime() string {
	return dateTime
}

func GetGoVersion() string {
	return goVersion
}
