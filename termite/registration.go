package termite

type Registration struct {
	Address string
	Name    string
	// TODO - hash of the secret?
}

type Registered struct {
	Registrations []Registration
}
