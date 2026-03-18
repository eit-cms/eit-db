package db

import "testing"

type LocationTaggedUser struct {
	ID  int    `eit_db:"id,primary_key"`
	Geo string `eit_db:"geo,type=location"`
}

func TestMapLocationTypeAcrossAdapters(t *testing.T) {
	if got := mapPostgresType(TypeLocation, nil); got != "POINT" {
		t.Fatalf("expected postgres POINT, got %s", got)
	}
	if got := mapMySQLType(TypeLocation); got != "POINT" {
		t.Fatalf("expected mysql POINT, got %s", got)
	}
	if got := mapSQLiteType(TypeLocation); got != "TEXT" {
		t.Fatalf("expected sqlite TEXT fallback, got %s", got)
	}
	if got := mapSQLServerType(TypeLocation); got != "GEOGRAPHY" {
		t.Fatalf("expected sqlserver GEOGRAPHY, got %s", got)
	}
}

func TestInferSchemaLocationTypeOverride(t *testing.T) {
	schema, err := InferSchema(LocationTaggedUser{})
	if err != nil {
		t.Fatalf("InferSchema failed: %v", err)
	}

	geo := schema.GetField("geo")
	if geo == nil {
		t.Fatal("geo field not found")
	}
	if geo.Type != TypeLocation {
		t.Fatalf("expected geo field type location, got %s", geo.Type)
	}
}

func TestParseFieldTypeAliasLocation(t *testing.T) {
	aliases := []string{"location", "geo", "geography", "point", "geopoint"}
	for _, alias := range aliases {
		got, ok := parseFieldTypeAlias(alias)
		if !ok {
			t.Fatalf("expected alias %s to be supported", alias)
		}
		if got != TypeLocation {
			t.Fatalf("expected alias %s to map to TypeLocation, got %s", alias, got)
		}
	}
}
