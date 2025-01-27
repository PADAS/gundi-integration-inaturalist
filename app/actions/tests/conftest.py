from uuid import UUID
from gundi_client_v2.client import IntegrationType
from gundi_core.schemas.v2 import ConnectionRoute, Integration, IntegrationAction, IntegrationActionConfiguration, IntegrationActionSummary, Organization
import pytest
import datetime
from dateutil.tz import tzoffset

@pytest.fixture
def inaturalist_integration_v2():
    return Integration(id = UUID('f03ec73e-f3fe-41b6-8597-3eb89dde5ae1'), name = 'Test iNaturalist', type = IntegrationType(id = UUID('7ea7b0a7-71fc-4577-936d-268dd45137f0'), name = 'iNaturalist', value = 'inaturalist', description = 'Default type for integrations with Inaturalist', actions = [IntegrationAction(id = UUID('6e17fd45-903a-4cb2-bd61-ee2db7abf31a'), type = 'pull', name = 'Pull Events', value = 'pull_events', description = 'Inaturalist Pull Events action', action_schema = {
    'type': 'object',
    'title': 'PullEventsConfig',
    'examples': [{
        '': 47.5218082,
        'distance': 30,
        'longitude': -122.3864506,
        'num_days_default': 1
    }],
    'required': ['days_to_load'],
    'properties': {
        'taxa': {
            'type': 'array',
            'items': {
                'type': 'string'
            },
            'title': 'Taxa IDs',
            'description': 'List of iNaturalist taxa IDs for which to load observations.'
        },
        'projects': {
            'type': 'array',
            'items': {
                'type': 'string'
            },
            'title': 'Project IDs',
            'description': 'List of project IDs to pull from iNaturalist.'
        },
        'event_type': {
            'type': 'string',
            'title': 'Event type',
            'default': 'inat_observation',
            'description': 'The event type to use in the returned event data.'
        },
        'annotations': {
            'type': 'string',
            'title': 'Annotations',
            'default': '',
            'description': 'Map of annotation terms and the values which to include.  For example, {"22": ["24", "25"], "1": ["2"]} would only include observations of Adults (annotation 1 == 2) that had the Evidence of Presence annotation (22) set to Organism (24) or Scat (25).  Entries in the Dict are treated as ORs, whereas values in the Lists are treated as ANDs.'
        },
        'bounding_box': {
            'type': 'string',
            'title': 'Bounding box for search area.  Of the format [ne_latitude, ne_longitude, sw_latitude, sw_longitude]',
            'default': ''
        },
        'days_to_load': {
            'type': 'integer',
            'title': 'Default number of days to load',
            'default': 5,
            'description': 'The number of days of data to load from iNaturalist.  If the integration state contains a last_run value, this parameter will be ignored and data will be loaded since the last_run value.'
        },
        'event_prefix': {
            'type': 'string',
            'title': 'Event prefix',
            'default': 'iNat: ',
            'description': "A string to prefix to the observed species to set a title when creating the event.  Default: 'iNat: '"
        },
        'quality_grade': {
            'type': 'array',
            'items': {
                'type': 'string'
            },
            'title': 'Quality Grade',
            'description': 'If present, only observations that have one of the entered quality grades will be included.  As of November, 2024, valid iNaturalist values are casual, needs_id and/or research.'
        },
        'include_photos': {
            'type': 'boolean',
            'title': 'Include photos',
            'default': True,
            'description': 'Whether or not to include the photos from iNaturalist observations.  Default: True'
        }
    },
    'definitions': {}
}, ui_schema = {})], webhook = None), base_url = '', enabled = True, owner = Organization(id = UUID('9d0ceb0f-648f-4ffa-91ea-f2973796ffa2'), name = 'Chicago Parks', description = ''), configurations = [IntegrationActionConfiguration(id = UUID('39e0a6db-c7cd-4ab1-a9d8-13416eee04ab'), integration = UUID('f03ec73e-f3fe-41b6-8597-3eb89dde5ae1'), action = IntegrationActionSummary(id = UUID('6e17fd45-903a-4cb2-bd61-ee2db7abf31a'), type = 'pull', name = 'Pull Events', value = 'pull_events'), data = {
    'taxa': [],
    'projects': [],
    'event_type': 'inat_observation',
    'annotations': '',
    'bounding_box': '[-27.698830, 48.472099, -27.759075, -48.527430]',
    'days_to_load': 5,
    'event_prefix': 'iNat: ',
    'quality_grade': [],
    'include_photos': True
})], webhook_configuration = None, default_route = ConnectionRoute(id = UUID('a3a1f096-43fe-4102-8742-bd4f2c539a4d'), name = 'Test iNaturalist - Default Route'), additional = {}, status = 'healthy', status_details = 'No issues detected')


@pytest.fixture
def inaturalist_integration_v2_without_bounding_box():
    return Integration(id = UUID('f03ec73e-f3fe-41b6-8597-3eb89dde5ae1'), name = 'Test iNaturalist', type = IntegrationType(id = UUID('7ea7b0a7-71fc-4577-936d-268dd45137f0'), name = 'iNaturalist', value = 'inaturalist', description = 'Default type for integrations with Inaturalist', actions = [IntegrationAction(id = UUID('6e17fd45-903a-4cb2-bd61-ee2db7abf31a'), type = 'pull', name = 'Pull Events', value = 'pull_events', description = 'Inaturalist Pull Events action', action_schema = {
    'type': 'object',
    'title': 'PullEventsConfig',
    'examples': [{
        '': 47.5218082,
        'distance': 30,
        'longitude': -122.3864506,
        'num_days_default': 1
    }],
    'required': ['days_to_load'],
    'properties': {
        'taxa': {
            'type': 'array',
            'items': {
                'type': 'string'
            },
            'title': 'Taxa IDs',
            'description': 'List of iNaturalist taxa IDs for which to load observations.'
        },
        'projects': {
            'type': 'array',
            'items': {
                'type': 'string'
            },
            'title': 'Project IDs',
            'description': 'List of project IDs to pull from iNaturalist.'
        },
        'event_type': {
            'type': 'string',
            'title': 'Event type',
            'default': 'inat_observation',
            'description': 'The event type to use in the returned event data.'
        },
        'annotations': {
            'type': 'string',
            'title': 'Annotations',
            'default': '',
            'description': 'Map of annotation terms and the values which to include.  For example, {"22": ["24", "25"], "1": ["2"]} would only include observations of Adults (annotation 1 == 2) that had the Evidence of Presence annotation (22) set to Organism (24) or Scat (25).  Entries in the Dict are treated as ORs, whereas values in the Lists are treated as ANDs.'
        },
        'bounding_box': {
            'type': 'string',
            'title': 'Bounding box for search area.  Of the format [ne_latitude, ne_longitude, sw_latitude, sw_longitude]',
            'default': ''
        },
        'days_to_load': {
            'type': 'integer',
            'title': 'Default number of days to load',
            'default': 5,
            'description': 'The number of days of data to load from iNaturalist.  If the integration state contains a last_run value, this parameter will be ignored and data will be loaded since the last_run value.'
        },
        'event_prefix': {
            'type': 'string',
            'title': 'Event prefix',
            'default': 'iNat: ',
            'description': "A string to prefix to the observed species to set a title when creating the event.  Default: 'iNat: '"
        },
        'quality_grade': {
            'type': 'array',
            'items': {
                'type': 'string'
            },
            'title': 'Quality Grade',
            'description': 'If present, only observations that have one of the entered quality grades will be included.  As of November, 2024, valid iNaturalist values are casual, needs_id and/or research.'
        },
        'include_photos': {
            'type': 'boolean',
            'title': 'Include photos',
            'default': True,
            'description': 'Whether or not to include the photos from iNaturalist observations.  Default: True'
        }
    },
    'definitions': {}
}, ui_schema = {})], webhook = None), base_url = '', enabled = True, owner = Organization(id = UUID('9d0ceb0f-648f-4ffa-91ea-f2973796ffa2'), name = 'Chicago Parks', description = ''), configurations = [IntegrationActionConfiguration(id = UUID('39e0a6db-c7cd-4ab1-a9d8-13416eee04ab'), integration = UUID('f03ec73e-f3fe-41b6-8597-3eb89dde5ae1'), action = IntegrationActionSummary(id = UUID('6e17fd45-903a-4cb2-bd61-ee2db7abf31a'), type = 'pull', name = 'Pull Events', value = 'pull_events'), data = {
    'taxa': [],
    'projects': [],
    'event_type': 'inat_observation',
    'annotations': '',
    'bounding_box': '',
    'days_to_load': 5,
    'event_prefix': 'iNat: ',
    'quality_grade': [],
    'include_photos': True
})], webhook_configuration = None, default_route = ConnectionRoute(id = UUID('a3a1f096-43fe-4102-8742-bd4f2c539a4d'), name = 'Test iNaturalist - Default Route'), additional = {}, status = 'healthy', status_details = 'No issues detected')


@pytest.fixture
def mock_get_observations_v2(mocker, 
                             mock_get_observations_v2_first_response,
                             mock_get_observations_v2_page_response,
                             mock_get_observations_v2_empty_page_response):
    mock = mocker.MagicMock()
    mock.side_effect = [mock_get_observations_v2_first_response,
                        mock_get_observations_v2_page_response,
                        mock_get_observations_v2_empty_page_response]
    return mock


@pytest.fixture
def mock_get_observations_v2_page_response():
    return {
        'total_results': 7,
        'page': 1,
        'per_page': 200,
        'results': [{
            'uuid': 'cb48a064-41be-4d18-8b82-1fc28eb68e27',
            'observed_on': '2024-09-23',
            'created_at': datetime.datetime(2024, 10, 3, 12, 53, 4, tzinfo = tzoffset(None, 7200)),
            'id': 245377839,
            'captive': False,
            'obscured': False,
            'place_guess': 'Karas Region, Namibia',
            'quality_grade': 'research',
            'species_guess': "lizard's-tail",
            'updated_at': datetime.datetime(2024, 12, 17, 23, 0, 23, tzinfo = tzoffset(None, 7200)),
            'uri': 'https://www.inaturalist.org/observations/245377839',
            'photos': [{
                'id': 437821149,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/437821149/square.jpeg'
            }, {
                'id': 437821146,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/437821146/square.jpeg'
            }, {
                'id': 437821155,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/437821155/square.jpeg'
            }, {
                'id': 437821159,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/437821159/square.jpeg'
            }],
            'user': {
                'id': 6117398,
                'name': 'Nogga_Eugene',
                'login': 'eugenemarais'
            },
            'location': [-27.7207507068, 16.7136730981],
            'place_ids': [7140, 13137, 16961, 59647, 66872, 91708, 97392, 113055, 122284, 123067, 131363, 184184, 201924],
            'taxon': {
                'id': 120255,
                'rank': 'species',
                'name': 'Crassula muscosa',
                'preferred_common_name': "lizard's-tail",
                'wikipedia_url': 'http://en.wikipedia.org/wiki/Crassula_muscosa'
            },
            'annotations': []
        }, {
            'uuid': 'd86a315b-09fa-46a8-ab18-60f3f48cf673',
            'observed_on': '2013-08-18',
            'created_at': datetime.datetime(2024, 12, 17, 1, 16, 57, tzinfo = tzoffset(None, 7200)),
            'id': 255337607,
            'captive': False,
            'obscured': True,
            'place_guess': 'Umkhanyakude, ZA-NL, ZA',
            'quality_grade': 'research',
            'species_guess': 'Southern Greater Kudu',
            'updated_at': datetime.datetime(2024, 12, 17, 23, 1, 20, tzinfo = tzoffset(None, 7200)),
            'uri': 'https://www.inaturalist.org/observations/255337607',
            'photos': [{
                'id': 457563391,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/457563391/square.jpeg'
            }],
            'user': {
                'id': 2405059,
                'name': 'Ryan Zucker',
                'login': 'birderryan'
            },
            'location': [-27.7260057075, 32.2037894977],
            'place_ids': [6986, 13313, 48462, 59647, 91708, 97392, 108116, 113055, 123370, 124078, 124432, 128956, 143633, 200412, 204528],
            'taxon': {
                'id': 524159,
                'rank': 'subspecies',
                'name': 'Tragelaphus strepsiceros strepsiceros',
                'preferred_common_name': 'Southern Greater Kudu',
                'wikipedia_url': 'http://en.wikipedia.org/wiki/Greater_kudu'
            },
            'annotations': []
        }, {
            'uuid': 'bbde6063-fb1c-49ff-8d99-760cd4d88ca2',
            'observed_on': '2024-12-12',
            'created_at': datetime.datetime(2024, 12, 12, 21, 10, 43, tzinfo = tzoffset(None, 7200)),
            'id': 254919143,
            'captive': False,
            'obscured': False,
            'place_guess': 'Aquamarine Drive, Newcastle, KwaZulu-Natal, ZA',
            'quality_grade': 'research',
            'species_guess': 'African Hoopoe',
            'updated_at': datetime.datetime(2024, 12, 18, 7, 58, 10, tzinfo = tzoffset(None, 7200)),
            'uri': 'https://www.inaturalist.org/observations/254919143',
            'photos': [{
                'id': 456720465,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/456720465/square.jpg'
            }, {
                'id': 456720475,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/456720475/square.jpg'
            }],
            'user': {
                'id': 5368241,
                'name': 'Angus Burns',
                'login': 'angus_burns'
            },
            'location': [-27.7227166759, 29.9557099215],
            'place_ids': [6986, 13313, 48450, 59647, 91708, 97392, 108109, 113055, 124432, 130180, 176808],
            'taxon': {
                'id': 142774,
                'rank': 'subspecies',
                'name': 'Upupa epops africana',
                'preferred_common_name': 'African Hoopoe',
                'wikipedia_url': None
            },
            'annotations': []
        }, {
            'uuid': 'b4976d6b-8dd3-4c19-ac0e-2a7399f3e186',
            'observed_on': '2013-08-17',
            'created_at': datetime.datetime(2024, 12, 17, 1, 16, 56, tzinfo = tzoffset(None, 7200)),
            'id': 255337603,
            'captive': False,
            'obscured': True,
            'place_guess': 'Umkhanyakude, ZA-NL, ZA',
            'quality_grade': 'research',
            'species_guess': 'White-throated monitor',
            'updated_at': datetime.datetime(2024, 12, 18, 15, 8, 2, tzinfo = tzoffset(None, 7200)),
            'uri': 'https://www.inaturalist.org/observations/255337603',
            'photos': [{
                'id': 457563078,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/457563078/square.jpeg'
            }],
            'user': {
                'id': 2405059,
                'name': 'Ryan Zucker',
                'login': 'birderryan'
            },
            'location': [-27.7437748205, 32.2728600045],
            'place_ids': [6986, 13313, 48462, 59647, 91708, 97392, 108116, 113055, 123370, 124078, 124432, 128956, 143633, 200412, 204528],
            'taxon': {
                'id': 116293,
                'rank': 'subspecies',
                'name': 'Varanus albigularis albigularis',
                'preferred_common_name': 'White-throated monitor',
                'wikipedia_url': 'http://en.wikipedia.org/wiki/White-throated_monitor'
            },
            'annotations': []
        }, {
            'uuid': 'b3458fe7-a6d4-412a-b467-4dd726456aaf',
            'observed_on': '2024-03-30',
            'created_at': datetime.datetime(2024, 4, 1, 18, 18, 43, tzinfo = tzoffset(None, 7200)),
            'id': 204943401,
            'captive': False,
            'obscured': False,
            'place_guess': 'Thabo Mofutsanyana District Municipality, South Africa',
            'quality_grade': 'research',
            'species_guess': 'Scarlet River Lily',
            'updated_at': datetime.datetime(2024, 12, 18, 17, 59, 51, tzinfo = tzoffset(None, 7200)),
            'uri': 'https://www.inaturalist.org/observations/204943401',
            'photos': [{
                'id': 362306804,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/362306804/square.jpg'
            }],
            'user': {
                'id': 1124721,
                'name': None,
                'login': 'stevenwevans'
            },
            'location': [-27.7134782503, 29.688618521],
            'place_ids': [6986, 12514, 48622, 50327, 59647, 91708, 97392, 108113, 113055, 124432, 130180, 176808],
            'taxon': {
                'id': 367209,
                'rank': 'species',
                'name': 'Hesperantha coccinea',
                'preferred_common_name': 'Scarlet River Lily',
                'wikipedia_url': 'http://en.wikipedia.org/wiki/Hesperantha_coccinea'
            },
            'annotations': []
        }, {
            'uuid': 'b15bf2ee-f8d5-4790-b77c-b244dabedadc',
            'observed_on': '2024-12-18',
            'created_at': datetime.datetime(2024, 12, 18, 13, 9, tzinfo = tzoffset(None, -10800)),
            'id': 255507124,
            'captive': False,
            'obscured': False,
            'place_guess': 'Pântano do Sul, Florianópolis - SC, Brasil',
            'quality_grade': 'needs_id',
            'species_guess': None,
            'updated_at': datetime.datetime(2024, 12, 18, 13, 28, 22, tzinfo = tzoffset(None, -10800)),
            'uri': 'https://www.inaturalist.org/observations/255507124',
            'photos': [{
                'id': 457891655,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/457891655/square.jpeg'
            }, {
                'id': 457891669,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/457891669/square.jpeg'
            }],
            'user': {
                'id': 8830853,
                'name': 'José Bonini',
                'login': 'josebonini'
            },
            'location': [-27.7379723772, -48.5165359452],
            'place_ids': [6878, 7994, 24546, 56773, 66741, 97389, 160620, 179769, 184033],
            'taxon': {
                'id': 47961,
                'rank': 'family',
                'name': 'Cerambycidae',
                'preferred_common_name': 'Longhorn Beetles',
                'wikipedia_url': 'http://en.wikipedia.org/wiki/Longhorn_beetle'
            },
            'annotations': []
        }, {
            'uuid': '05f084c1-8de3-4ef7-a04b-9f2a2a0ab469',
            'observed_on': '2024-12-18',
            'created_at': datetime.datetime(2024, 12, 18, 12, 16, 50, tzinfo = tzoffset(None, -10800)),
            'id': 255502488,
            'captive': False,
            'obscured': False,
            'place_guess': 'Ilha de Santa Catarina, Florianópolis, SC, BR',
            'quality_grade': 'needs_id',
            'species_guess': 'Araneus unanimus',
            'updated_at': datetime.datetime(2024, 12, 18, 15, 25, 8, tzinfo = tzoffset(None, -10800)),
            'uri': 'https://www.inaturalist.org/observations/255502488',
            'photos': [{
                'id': 457884562,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/457884562/square.jpg'
            }, {
                'id': 457884569,
                'url': 'https://inaturalist-open-data.s3.amazonaws.com/photos/457884569/square.jpg'
            }],
            'user': {
                'id': 7305554,
                'name': None,
                'login': 'jordisanchez'
            },
            'location': [-27.7115717088, -48.5021505587],
            'place_ids': [6878, 7994, 24546, 56773, 66741, 97389, 160620, 179769],
            'taxon': {
                'id': 810393,
                'rank': 'species',
                'name': 'Araneus unanimus',
                'wikipedia_url': None
            },
            'annotations': []
        }]
    }


@pytest.fixture
def mock_get_observations_v2_first_response():
    return {
        'total_results': 16,
        'page': 1,
        'per_page': 0,
        'results': []
    }


@pytest.fixture
def mock_get_observations_v2_empty_page_response():
    return {
        'total_results': 1,
        'page': 3,
        'per_page': 200,
        'results': []
    }

