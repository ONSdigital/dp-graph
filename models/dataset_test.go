package models

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDataset_UpdateLinks(t *testing.T) {

	Convey("Given a Datasets with Dataset Item containing a Link and an Edition", t, func() {

		datasetAPIhost := "datasetAPIHost"
		editionID := "editionID"

		ds := Datasets{
			Items: []Dataset{
				Dataset{
					Links: &DatasetLinks{
						&Link{
							ID:   "datasetLink1",
							Href: "datasetHref1",
						},
					},
					DimensionLabel: "dimLabel1",
					Editions: []DatasetEdition{
						DatasetEdition{Links: &DatasetEditionLinks{
							Self: &Link{
								ID:   "editionSelfLink1",
								Href: "editionSelfHref1",
							},
							DatasetDimension: &Link{
								ID:   "editionDatasetLink1",
								Href: "editionDatasetHref1",
							},
							LatestVersion: &Link{
								ID:   "editionLatestLink1",
								Href: "editionLatestHref1",
							},
						}},
					},
				},
			},
		}

		Convey("When UpdateLinks is called, then the links are updated accordingly", func() {
			expectedItemsAfter := []Dataset{
				Dataset{
					Links: &DatasetLinks{
						&Link{
							ID:   "datasetLink1",
							Href: "datasetAPIHost/datasets/datasetLink1",
						},
					},
					DimensionLabel: "dimLabel1",
					Editions: []DatasetEdition{
						DatasetEdition{
							Links: &DatasetEditionLinks{
								Self: &Link{
									ID:   "editionSelfLink1",
									Href: "datasetAPIHost/datasets/datasetLink1/editions/editionSelfLink1",
								},
								DatasetDimension: &Link{
									ID:   "editionDatasetLink1",
									Href: "datasetAPIHost/datasets/datasetLink1/editions/editionSelfLink1/versions/editionLatestLink1/dimensions/editionDatasetLink1",
								},
								LatestVersion: &Link{
									ID:   "editionLatestLink1",
									Href: "datasetAPIHost/datasets/datasetLink1/editions/editionSelfLink1/versions/editionLatestLink1",
								},
							},
						},
					},
				},
			}

			err := ds.UpdateLinks(datasetAPIhost, editionID)
			So(err, ShouldBeNil)
			So(ds.Items, ShouldResemble, expectedItemsAfter)
		})
	})

}
