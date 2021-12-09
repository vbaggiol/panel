import { HTMLBox } from "@bokehjs/models/layouts/html_box";
import * as p from "@bokehjs/core/properties";
import { ColumnDataSource } from "@bokehjs/models/sources/column_data_source";
import { TableColumn } from "@bokehjs/models/widgets/tables";
import { PanelHTMLBoxView } from "./layout";
export declare class DataTabulatorView extends PanelHTMLBoxView {
    model: DataTabulator;
    tabulator: any;
    _tabulator_cell_updating: boolean;
    _data_updating: boolean;
    _selection_updating: boolean;
    _styled_cells: any[];
    _styles: any;
    _initializing: boolean;
    connect_signals(): void;
    render_and_resize(): void;
    render(): void;
    tableInit(view: DataTabulatorView, tabulator: any): void;
    requestPage(page: number, sorters: any[]): Promise<void>;
    renderComplete(): void;
    freezeRows(): void;
    getLayout(): string;
    getConfiguration(): any;
    getColumns(): any;
    renderEditor(column: any, cell: any, onRendered: any, success: any, error: any): any;
    after_layout(): void;
    setData(): void;
    setGroupBy(): void;
    setCSS(): boolean;
    updateStyles(): void;
    addData(): void;
    updateOrAddData(): void;
    hideColumns(): void;
    setMaxPage(): void;
    setPage(): void;
    setPageSize(): void;
    updateSelection(): void;
    rowClicked(e: any, row: any): void;
    _filter_selected(indices: number[]): number[];
    rowSelectionChanged(data: any, _: any): void;
    cellEdited(cell: any): void;
}
export declare const TableLayout: import("@bokehjs/core/kinds").Kinds.Enum<"fit_data" | "fit_data_fill" | "fit_data_stretch" | "fit_data_table" | "fit_columns">;
export declare namespace DataTabulator {
    type Attrs = p.AttrsOf<Props>;
    type Props = HTMLBox.Props & {
        aggregators: p.Property<any>;
        columns: p.Property<TableColumn[]>;
        configuration: p.Property<any>;
        download: p.Property<boolean>;
        filename: p.Property<string>;
        editable: p.Property<boolean>;
        follow: p.Property<boolean>;
        frozen_rows: p.Property<number[]>;
        groupby: p.Property<string[]>;
        hidden_columns: p.Property<string[]>;
        indexes: p.Property<string[]>;
        layout: p.Property<typeof TableLayout["__type__"]>;
        max_page: p.Property<number>;
        page: p.Property<number>;
        page_size: p.Property<number>;
        pagination: p.Property<string | null>;
        select_mode: p.Property<any>;
        selectable_rows: p.Property<number[] | null>;
        source: p.Property<ColumnDataSource>;
        sorters: p.Property<any[]>;
        styles: p.Property<any>;
        theme: p.Property<string>;
        theme_url: p.Property<string>;
    };
}
export interface DataTabulator extends DataTabulator.Attrs {
}
export declare class DataTabulator extends HTMLBox {
    properties: DataTabulator.Props;
    constructor(attrs?: Partial<DataTabulator.Attrs>);
    static __module__: string;
    static init_DataTabulator(): void;
}
