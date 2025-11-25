# Dry-Run HTTP Payload Simulation

- Generated: 2025-11-25 15:28:33Z
- Base URL: `https://api.revelator.com`
- EnterpriseId: 691097
- TenantId: 332920

Full payload lists are exported as JSON under `artifacts/`. This document highlights the HTTP requests that a live run will send.

Key payload artifacts:
- `artifacts/artists.json`
- `artifacts/labels.json`
- `artifacts/publishers.json`
- `artifacts/composers.json`
- `artifacts/releases.json`
- `artifacts/tracks.json`
- `artifacts/audio_urls.json`

Lookup endpoints called before mutations (GET):
- `https://api.revelator.com/content/label/all` (with pagination fallbacks)
- `https://api.revelator.com/content/publisher/all`
- `https://api.revelator.com/content/composer/all`
- `https://api.revelator.com/common/lookup/contributorRoles`
- `https://api.revelator.com/common/lookup/languages` (on demand)
- `https://api.revelator.com/common/lookup/musicstyles` (on demand)
- `https://api.revelator.com/common/lookup/countries` (on demand)
- `https://api.revelator.com/content/release/all` (live duplicate detection)
- `https://api.revelator.com/content/track/all` (live duplicate detection)

## Audio ingest (pull external)

- Method: `POST`
- Endpoint: Per track → /media/audio/pullexternal/{ext}
- Requests: 2
- Each ISRC uploads the referenced audio before release/track creation.
- The isrc field shown below is informational for this report and is not part of the HTTP request body.

Sample payloads:
```json
[
  {
    "endpoint": "https://api.revelator.com/media/audio/pullexternal/wav",
    "body": {
      "externalUrl": "https://www.dropbox.com/scl/fi/sjv55c5eqq0utzgy15ojq/Joy-Mejia-Amarte-Es-Mi-Pasatiempo.wav?rlkey=6j5ruxil4gc3vxonuf8ownxvi&st=8kysye3f&dl=1",
      "fileName": "Joy-Mejia-Amarte-Es-Mi-Pasatiempo.wav"
    },
    "isrc": "QZW9M2221000"
  },
  {
    "endpoint": "https://api.revelator.com/media/audio/pullexternal/wav",
    "body": {
      "externalUrl": "https://www.dropbox.com/scl/fi/ncgo2nf5srfafm0odhequ/Joy-Mejia-Te-Fall.wav?rlkey=6jl64vr3v0w4tn6km1sbhlxux&st=um08pokf&dl=1",
      "fileName": "Joy-Mejia-Te-Fall.wav"
    },
    "isrc": "QZW9M2236571"
  }
]
```

## Artist profile image uploads

- Method: `POST`
- Endpoint: Per artist → /media/image/upload?cover=false
- Requests: 2
- Images are uploaded prior to calling /artists. Live runs send multipart form-data with the listed filename.

Sample payloads:
```json
[
  {
    "endpoint": "https://api.revelator.com/media/image/upload?cover=false",
    "artistName": "JKZTest",
    "filename": "Receba-789294845633-UPC.jpg",
    "sourceUrl": "https://www.dropbox.com/scl/fi/7safoetk4p2k4vwi29k4c/Receba-789294845633-UPC.jpg?rlkey=3vty6hb78ol10iv3y6h4md8h3&st=m30ox0p4&dl=1"
  },
  {
    "endpoint": "https://api.revelator.com/media/image/upload?cover=false",
    "artistName": "Flash Beats ManowTest",
    "filename": "A-Cl-usula-do-Contrato-789294962088-UPC.jpg",
    "sourceUrl": "https://www.dropbox.com/scl/fi/wky0ryb739s0xs3wo5qop/A-Cl-usula-do-Contrato-789294962088-UPC.jpg?rlkey=91bp85llgb8ho5lnfcs56105s&st=dn12pxrd&dl=1"
  }
]
```

## Upsert artists

- Method: `POST`
- Endpoint: https://api.revelator.com/artists
- Requests: 2
- During live execution, the placeholder image.sourceUrl shown here is replaced once the upload stage returns image.fileId values.

Sample payloads:
```json
[
  {
    "endpoint": "https://api.revelator.com/artists",
    "body": {
      "name": "JKZTest",
      "artistExternalIds": [
        {
          "distributorStoreId": 1,
          "profileId": "1532352644"
        },
        {
          "distributorStoreId": 9,
          "profileId": "1Gn8eEXtOWHA7XU8wEWXOb"
        },
        {
          "distributorStoreId": 309,
          "profileId": "697315585919469"
        }
      ],
      "isni": "0000000121032683",
      "image": {
        "filename": "Receba-789294845633-UPC.jpg",
        "fileId": null
      }
    }
  },
  {
    "endpoint": "https://api.revelator.com/artists",
    "body": {
      "name": "Flash Beats ManowTest",
      "artistExternalIds": [
        {
          "distributorStoreId": 1,
          "profileId": "1601139620"
        },
        {
          "distributorStoreId": 9,
          "profileId": "0ZIlwtVZkqtMZUpbHrz8TL"
        },
        {
          "distributorStoreId": 309,
          "profileId": "697315585919468"
        }
      ],
      "image": {
        "filename": "A-Cl-usula-do-Contrato-789294962088-UPC.jpg",
        "fileId": null
      }
    }
  }
]
```

## Save labels

- Method: `POST`
- Endpoint: https://api.revelator.com/content/label/save
- Requests: 2
- Existing labels are detected via GET /content/label/all; only unknown names trigger POST requests.

Sample payloads:
```json
[
  {
    "endpoint": "https://api.revelator.com/content/label/save",
    "body": {
      "name": "TokyoRecordsFULLTEST"
    }
  },
  {
    "endpoint": "https://api.revelator.com/content/label/save",
    "body": {
      "name": "PEJOTA10*FULLTEST"
    }
  }
]
```

## Save publishers

- Method: `POST`
- Endpoint: https://api.revelator.com/content/publisher/save
- Requests: 2

Sample payloads:
```json
[
  {
    "endpoint": "https://api.revelator.com/content/publisher/save",
    "body": {
      "name": "SONY ATV",
      "publisherId": 90322,
      "ipiCae": "00006718450",
      "_ipi_source_col": null,
      "countryId": 900
    }
  },
  {
    "endpoint": "https://api.revelator.com/content/publisher/save",
    "body": {
      "name": "WARNER CHAPPEL",
      "publisherId": 90323
    }
  }
]
```

## Save composers

- Method: `POST`
- Endpoint: https://api.revelator.com/content/composer/save
- Requests: 2

Sample payloads:
```json
[
  {
    "endpoint": "https://api.revelator.com/content/composer/save",
    "body": {
      "name": "Agnaldo Paulino de Jesus Júnior",
      "composerId": 1440495,
      "isni": "0000000121032684",
      "ipiCae": "00006718449",
      "countryOfResidenceId": 900
    }
  },
  {
    "endpoint": "https://api.revelator.com/content/composer/save",
    "body": {
      "name": "Jackson Bertholdo Dos Santos",
      "composerId": 1440496,
      "isni": "0000000121032685"
    }
  }
]
```

## Release cover image uploads

- Method: `POST`
- Endpoint: Per release → /media/image/upload?cover=true
- Requests: 2
- Successful uploads replace imageSourceUrl with image.fileId inside the release payload prior to /content/release/save.

Sample payloads:
```json
[
  {
    "endpoint": "https://api.revelator.com/media/image/upload?cover=true",
    "releaseName": "Receba",
    "filename": "Receba-789294845633-UPC.jpg",
    "sourceUrl": "https://www.dropbox.com/scl/fi/7safoetk4p2k4vwi29k4c/Receba-789294845633-UPC.jpg?rlkey=3vty6hb78ol10iv3y6h4md8h3&st=m30ox0p4&dl=1"
  },
  {
    "endpoint": "https://api.revelator.com/media/image/upload?cover=true",
    "releaseName": "A Cláusula do Contrato",
    "filename": "A-Cl-usula-do-Contrato-789294962088-UPC.jpg",
    "sourceUrl": "https://www.dropbox.com/scl/fi/wky0ryb739s0xs3wo5qop/A-Cl-usula-do-Contrato-789294962088-UPC.jpg?rlkey=91bp85llgb8ho5lnfcs56105s&st=dn12pxrd&dl=1"
  }
]
```

## Create releases

- Method: `POST`
- Endpoint: https://api.revelator.com/content/release/save
- Requests: 2
- On duplicate UPC responses, the script retries without the UPC value (see live logs).

Sample payloads:
```json
[
  {
    "endpoint": "https://api.revelator.com/content/release/save",
    "body": {
      "name": "Receba",
      "version": "Extended",
      "previouslyReleased": true,
      "releaseDate": "2022-11-21 00:00:00",
      "upc": "999294845633",
      "copyrightP": "2022 TokyoP",
      "copyrightC": "2022 TokyoC",
      "releaseLocals": [
        {
          "languageId": 31,
          "name": "Receba"
        }
      ],
      "languageId": 31,
      "primaryMusicStyleId": 41,
      "secondaryMusicStyleId": 555,
      "hasRecordLabel": true,
      "labelName": "TokyoRecordsFULLTEST",
      "imageSourceUrl": "https://www.dropbox.com/scl/fi/7safoetk4p2k4vwi29k4c/Receba-789294845633-UPC.jpg?rlkey=3vty6hb78ol10iv3y6h4md8h3&st=m30ox0p4&dl=1",
      "artistExternalIds": [
        {
          "distributorStoreId": 1,
          "profileId": "1532352644"
        },
        {
          "distributorStoreId": 9,
          "profileId": "1Gn8eEXtOWHA7XU8wEWXOb"
        },
        {
          "distributorStoreId": 309,
          "profileId": "697315585919469"
        }
      ],
      "artistName": "JKZTest",
      "contributors": [
        {
          "roleId": 34,
          "artist": {
            "name": "JKZTest",
            "artistExternalIds": [
              {
                "distributorStoreId": 1,
                "profileId": "1532352644"
              },
              {
                "distributorStoreId": 9,
                "profileId": "1Gn8eEXtOWHA7XU8wEWXOb"
              },
              {
                "distributorStoreId": 309,
                "profileId": "697315585919469"
              }
            ],
            "isni": "0000000121032683",
            "image": {
              "filename": "Receba-789294845633-UPC.jpg",
              "fileId": null
            }
          }
        },
        {
          "roleId": 19,
          "artist": {
            "name": "JKZTest",
            "artistExternalIds": [
              {
                "distributorStoreId": 1,
                "profileId": "1532352644"
              },
              {
                "distributorStoreId": 9,
                "profileId": "1Gn8eEXtOWHA7XU8wEWXOb"
              },
              {
                "distributorStoreId": 309,
                "profileId": "697315585919469"
              }
            ],
            "isni": "0000000121032683",
            "image": {
              "filename": "Receba-789294845633-UPC.jpg",
              "fileId": null
            }
          }
        }
      ]
    }
  },
  {
    "endpoint": "https://api.revelator.com/content/release/save",
    "body": {
      "name": "A Cláusula do Contrato",
      "version": null,
      "previouslyReleased": true,
      "releaseDate": "2022-11-24 00:00:00",
      "upc": "999294962088",
      "copyrightP": "2022 PEJOTAP",
      "copyrightC": "2022 PEJOTAC",
      "releaseLocals": [
        {
          "languageId": 31,
          "name": "A Cláusula do Contrato"
        }
      ],
      "languageId": 31,
      "primaryMusicStyleId": 41,
      "secondaryMusicStyleId": 555,
      "hasRecordLabel": true,
      "labelName": "PEJOTA10*FULLTEST",
      "imageSourceUrl": "https://www.dropbox.com/scl/fi/wky0ryb739s0xs3wo5qop/A-Cl-usula-do-Contrato-789294962088-UPC.jpg?rlkey=91bp85llgb8ho5lnfcs56105s&st=dn12pxrd&dl=1",
      "artistExternalIds": [
        {
          "distributorStoreId": 1,
          "profileId": "1601139620"
        },
        {
          "distributorStoreId": 9,
          "profileId": "0ZIlwtVZkqtMZUpbHrz8TL"
        },
        {
          "distributorStoreId": 309,
          "profileId": "697315585919468"
        }
      ],
      "artistName": "Flash Beats ManowTest",
      "contributors": [
        {
          "roleId": 34,
          "artist": {
            "name": "Flash Beats ManowTest",
            "artistExternalIds": [
              {
                "distributorStoreId": 1,
                "profileId": "1601139620"
              },
              {
                "distributorStoreId": 9,
                "profileId": "0ZIlwtVZkqtMZUpbHrz8TL"
              },
              {
                "distributorStoreId": 309,
                "profileId": "697315585919468"
              }
            ],
            "image": {
              "filename": "A-Cl-usula-do-Contrato-789294962088-UPC.jpg",
              "fileId": null
            }
          }
        },
        {
          "roleId": 19,
          "artist": {
            "name": "Flash Beats ManowTest",
            "artistExternalIds": [
              {
                "distributorStoreId": 1,
                "profileId": "1601139620"
              },
              {
                "distributorStoreId": 9,
                "profileId": "0ZIlwtVZkqtMZUpbHrz8TL"
              },
              {
                "distributorStoreId": 309,
                "profileId": "697315585919468"
              }
            ],
            "image": {
              "filename": "A-Cl-usula-do-Contrato-789294962088-UPC.jpg",
              "fileId": null
            }
          }
        }
      ]
    }
  }
]
```

## Create tracks

- Method: `POST`
- Endpoint: https://api.revelator.com/content/track/save
- Requests: 2

Sample payloads:
```json
[
  {
    "endpoint": "https://api.revelator.com/content/track/save",
    "body": {
      "name": "Receba",
      "version": "Extended",
      "languageId": 31,
      "explicit": false,
      "trackType": 1,
      "trackNumber": 1,
      "previewStartSeconds": 140,
      "trackRecordingVersions": [
        {
          "isrc": "QZW9M2221000",
          "recordingVersionType": 1,
          "audioFiles": [
            {
              "audioId": "95173ab7-22e5-4d87-afb9-bbc04b116b5c",
              "audioFilename": "Joy-Mejia-Amarte-Es-Mi-Pasatiempo.wav",
              "fileFormat": 2
            }
          ]
        }
      ],
      "artistName": "JKZTest",
      "artistExternalIds": [
        {
          "distributorStoreId": 1,
          "profileId": "1532352644"
        },
        {
          "distributorStoreId": 9,
          "profileId": "1Gn8eEXtOWHA7XU8wEWXOb"
        },
        {
          "distributorStoreId": 309,
          "profileId": "697315585919469"
        }
      ],
      "trackProperties": [
        1
      ],
      "compositions": [
        {
          "iswc": "T1234567890"
        }
      ],
      "composerContentsDTO": [
        {
          "share": "60",
          "composerName": "Agnaldo Paulino de Jesus Júnior",
          "roleId": 39,
          "rightsId": 1,
          "isni": "0000000121032684",
          "ipiCae": "00006718449",
          "composersLocals": [
            {
              "languageId": 31,
              "name": "Agnaldo Paulino de Jesus Júnior",
              "version": "Extended"
            }
          ]
        },
        {
          "share": "40",
          "composerName": "Jackson Bertholdo Dos Santos",
          "roleId": 39,
          "rightsId": 1,
          "isni": "0000000121032685",
          "composersLocals": [
            {
              "languageId": 31,
              "name": "Jackson Bertholdo Dos Santos",
              "version": "Extended"
            }
          ]
        }
      ],
      "trackLocals": [
        {
          "name": "Receba",
          "languageId": 31,
          "version": "Extended"
        }
      ],
      "contributors": [
        {
          "roleId": 34,
          "artist": {
            "name": "JKZTest",
            "artistExternalIds": [
              {
                "distributorStoreId": 1,
                "profileId": "1532352644"
              },
              {
                "distributorStoreId": 9,
                "profileId": "1Gn8eEXtOWHA7XU8wEWXOb"
              },
              {
                "distributorStoreId": 309,
                "profileId": "697315585919469"
              }
            ],
            "isni": "0000000121032683",
            "image": {
              "filename": "Receba-789294845633-UPC.jpg",
              "fileId": null
            }
          }
        },
        {
          "roleId": 19,
          "artist": {
            "name": "JKZTest",
            "artistExternalIds": [
              {
                "distributorStoreId": 1,
                "profileId": "1532352644"
              },
              {
                "distributorStoreId": 9,
                "profileId": "1Gn8eEXtOWHA7XU8wEWXOb"
              },
              {
                "distributorStoreId": 309,
                "profileId": "697315585919469"
              }
            ],
            "isni": "0000000121032683",
            "image": {
              "filename": "Receba-789294845633-UPC.jpg",
              "fileId": null
            }
          }
        },
        {
          "roleId": 39,
          "artist": {
            "name": "Agnaldo Paulino de Jesus Júnior"
          }
        },
        {
          "roleId": 39,
          "artist": {
            "name": "Jackson Bertholdo Dos Santos"
          }
        }
      ]
    },
    "releaseUPC": "999294845633"
  },
  {
    "endpoint": "https://api.revelator.com/content/track/save",
    "body": {
      "name": "A Cláusula do Contrato",
      "version": null,
      "languageId": 31,
      "explicit": false,
      "trackType": 1,
      "trackNumber": 1,
      "previewStartSeconds": 200,
      "trackRecordingVersions": [
        {
          "isrc": "QZW9M2236571",
          "recordingVersionType": 1,
          "audioFiles": [
            {
              "audioId": "bd2a5de8-8a8e-4015-b2bb-c5414bbd9a7a",
              "audioFilename": "Joy-Mejia-Te-Fall.wav",
              "fileFormat": 2
            }
          ]
        }
      ],
      "artistName": "Flash Beats ManowTest",
      "artistExternalIds": [
        {
          "distributorStoreId": 1,
          "profileId": "1601139620"
        },
        {
          "distributorStoreId": 9,
          "profileId": "0ZIlwtVZkqtMZUpbHrz8TL"
        },
        {
          "distributorStoreId": 309,
          "profileId": "697315585919468"
        }
      ],
      "trackProperties": [
        1
      ],
      "compositions": [
        {
          "iswc": "T1234567890"
        }
      ],
      "composerContentsDTO": [
        {
          "share": "50",
          "composerName": "Jackson Bertholdo Dos Santos",
          "roleId": 39,
          "rightsId": 2,
          "isni": "0000000121032685",
          "publisherName": "SONY ATV",
          "publisherId": 90322,
          "composersLocals": [
            {
              "languageId": 31,
              "name": "Jackson Bertholdo Dos Santos"
            }
          ]
        },
        {
          "share": "50",
          "composerName": "Agnaldo Paulino de Jesus Júnior",
          "roleId": 39,
          "rightsId": 2,
          "isni": "0000000121032684",
          "ipiCae": "00006718449",
          "publisherName": "WARNER CHAPPEL",
          "publisherId": 90323,
          "composersLocals": [
            {
              "languageId": 31,
              "name": "Agnaldo Paulino de Jesus Júnior"
            }
          ]
        }
      ],
      "trackLocals": [
        {
          "name": "A Cláusula do Contrato",
          "languageId": 31
        }
      ],
      "contributors": [
        {
          "roleId": 34,
          "artist": {
            "name": "Flash Beats ManowTest",
            "artistExternalIds": [
              {
                "distributorStoreId": 1,
                "profileId": "1601139620"
              },
              {
                "distributorStoreId": 9,
                "profileId": "0ZIlwtVZkqtMZUpbHrz8TL"
              },
              {
                "distributorStoreId": 309,
                "profileId": "697315585919468"
              }
            ],
            "image": {
              "filename": "A-Cl-usula-do-Contrato-789294962088-UPC.jpg",
              "fileId": null
            }
          }
        },
        {
          "roleId": 19,
          "artist": {
            "name": "Flash Beats ManowTest",
            "artistExternalIds": [
              {
                "distributorStoreId": 1,
                "profileId": "1601139620"
              },
              {
                "distributorStoreId": 9,
                "profileId": "0ZIlwtVZkqtMZUpbHrz8TL"
              },
              {
                "distributorStoreId": 309,
                "profileId": "697315585919468"
              }
            ],
            "image": {
              "filename": "A-Cl-usula-do-Contrato-789294962088-UPC.jpg",
              "fileId": null
            }
          }
        },
        {
          "roleId": 39,
          "artist": {
            "name": "Jackson Bertholdo Dos Santos"
          }
        },
        {
          "roleId": 39,
          "artist": {
            "name": "Agnaldo Paulino de Jesus Júnior"
          }
        }
      ]
    },
    "releaseUPC": "999294962088"
  }
]
```
