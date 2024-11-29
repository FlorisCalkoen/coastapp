import param
import panel as pn

from coastapp.specification import BaseModel


class SharedState(param.Parameterized):
    """
    Centralized class for shared parameters across the app.
    """

    current_transect = param.ClassSelector(class_=BaseModel, doc="Current transect")
    only_use_incorrect = param.Boolean(
        default=False, doc="Only show incorrect predictions."
    )
    only_use_non_validated = param.Boolean(
        default=False, doc="Only show non-validated predictions."
    )
    show_labelled_transects = param.Boolean(
        default=False, doc="Show/Hide Labelled Transects"
    )
    show_test_predictions = param.Boolean(
        default=False, doc="Show/Hide Test Prediction Layer"
    )
    use_test_storage_backend = param.Boolean(
        default=False, doc="Use test storage backend"
    )
    confidence_filter_slider = pn.widgets.DiscreteSlider(
        options=["low", "medium", "high"], name="Confidence Filter", value="medium"
    )
    seen_uuids = param.List(default=[], doc="List of UUIDs already seen.")


shared_state = SharedState()
