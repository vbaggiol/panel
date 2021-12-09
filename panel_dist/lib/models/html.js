import { Markup } from "@bokehjs/models/widgets/markup";
import { ModelEvent } from "@bokehjs/core/bokeh_events";
import { PanelMarkupView } from "./layout";
import { serializeEvent } from "./event-to-object";
export class DOMEvent extends ModelEvent {
    constructor(node, data) {
        super();
        this.node = node;
        this.data = data;
        this.event_name = "dom_event";
    }
    _to_json() {
        return { model: this.origin, node: this.node, data: this.data };
    }
}
DOMEvent.__name__ = "DOMEvent";
export function htmlDecode(input) {
    var doc = new DOMParser().parseFromString(input, "text/html");
    return doc.documentElement.textContent;
}
export function runScripts(node) {
    Array.from(node.querySelectorAll("script")).forEach((oldScript) => {
        const newScript = document.createElement("script");
        Array.from(oldScript.attributes)
            .forEach((attr) => newScript.setAttribute(attr.name, attr.value));
        newScript.appendChild(document.createTextNode(oldScript.innerHTML));
        if (oldScript.parentNode)
            oldScript.parentNode.replaceChild(newScript, oldScript);
    });
}
export class HTMLView extends PanelMarkupView {
    constructor() {
        super(...arguments);
        this._event_listeners = {};
    }
    connect_signals() {
        super.connect_signals();
        this.connect(this.model.properties.events.change, () => {
            this._remove_event_listeners();
            this._setup_event_listeners();
        });
    }
    render() {
        super.render();
        const decoded = htmlDecode(this.model.text);
        const html = decoded || this.model.text;
        if (!html) {
            this.markup_el.innerHTML = '';
            return;
        }
        this.markup_el.innerHTML = html;
        runScripts(this.markup_el);
        this._setup_event_listeners();
    }
    _remove_event_listeners() {
        for (const node in this._event_listeners) {
            const el = document.getElementById(node);
            if (el == null) {
                console.warn(`DOM node '${node}' could not be found. Cannot subscribe to DOM events.`);
                continue;
            }
            for (const event_name in this._event_listeners[node]) {
                const event_callback = this._event_listeners[node][event_name];
                el.removeEventListener(event_name, event_callback);
            }
        }
        this._event_listeners = {};
    }
    _setup_event_listeners() {
        for (const node in this.model.events) {
            const el = document.getElementById(node);
            if (el == null) {
                console.warn(`DOM node '${node}' could not be found. Cannot subscribe to DOM events.`);
                continue;
            }
            for (const event_name of this.model.events[node]) {
                const callback = (event) => {
                    this.model.trigger_event(new DOMEvent(node, serializeEvent(event)));
                };
                el.addEventListener(event_name, callback);
                if (!(node in this._event_listeners))
                    this._event_listeners[node] = {};
                this._event_listeners[node][event_name] = callback;
            }
        }
    }
}
HTMLView.__name__ = "HTMLView";
export class HTML extends Markup {
    constructor(attrs) {
        super(attrs);
    }
    static init_HTML() {
        this.prototype.default_view = HTMLView;
        this.define(({ Any }) => ({
            events: [Any, {}]
        }));
    }
}
HTML.__name__ = "HTML";
HTML.__module__ = "panel.models.markup";
HTML.init_HTML();
//# sourceMappingURL=html.js.map