package main

func main() {
	rc := BuildConfig()
	switch rc.Command {
	case "query":
		queryMain(rc)

	}

}
