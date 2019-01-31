package config

import (
	"os"
	"testing"

	"github.com/ONSdigital/dp-graph/mock"
	"github.com/ONSdigital/dp-graph/neo4j"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetReturnsDefaultValues(t *testing.T) {
	Convey("When a loading a configuration, default values are returned", t, func() {
		cfg = nil
		cfg, err := Get()
		So(err, ShouldBeNil)
		So(cfg, ShouldNotBeNil)

		db, ok := (cfg.Driver).(*mock.Mock)
		So(ok, ShouldBeTrue)
		So(db, ShouldNotBeNil)

	})
}

func TestGetReturnsChosenDriver(t *testing.T) {
	Convey("When choosing the neo4j driver", t, func() {
		cfg = nil

		err := os.Setenv("GRAPH_DRIVER", "neo4j")
		So(err, ShouldBeNil)

		Convey("then the correct driver is returned", func() {
			cfg, err = Get()
			So(err, ShouldBeNil)
			So(cfg, ShouldNotBeNil)

			db, ok := (cfg.Driver).(*neo4j.Neo4j)
			So(ok, ShouldBeTrue)
			So(db, ShouldNotBeNil)
		})
	})
}
