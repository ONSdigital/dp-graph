package config

import (
	"os"
	"testing"

	"github.com/ONSdigital/dp-graph/v3/neo4j"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetFailsByDefault(t *testing.T) {
	Convey("When configuration not provided, fail by default", t, func() {
		os.Clearenv()
		cfg = nil
		cfg, err := Get(nil)
		So(err, ShouldNotBeNil)
		So(cfg, ShouldBeNil)

		So(err.Error(), ShouldEqual, "driver type config not provided")

	})
}

func TestGetReturnsChosenDriver(t *testing.T) {
	Convey("When choosing the neo4j driver", t, func() {
		cfg = nil

		err := os.Setenv("GRAPH_DRIVER_TYPE", "neo4j")
		So(err, ShouldBeNil)

		Convey("then the correct driver is returned", func() {
			cfg, err = Get(nil)
			So(err, ShouldBeNil)
			So(cfg, ShouldNotBeNil)

			db, ok := (cfg.Driver).(*neo4j.Neo4j)
			So(ok, ShouldBeTrue)
			So(db, ShouldNotBeNil)
		})
	})
}
