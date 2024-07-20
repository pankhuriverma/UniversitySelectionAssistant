import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { FormsModule } from '@angular/forms';

import { AppComponent } from './app.component';
import {AgGridModule} from "ag-grid-angular";
import { HomeComponent } from './home/home.component';
import { HomeService } from './home.service';
import { HttpClientModule } from '@angular/common/http';
import { TabMenuModule } from 'primeng/tabmenu';
import { StudentInfoComponent } from './student-info/student-info.component';
import { TableModule } from 'primeng/table';
import { UniversityDataComponent } from './university-data/university-data.component';
import { QueryComponent } from './query/query.component';
import { DropdownModule } from 'primeng/dropdown';
import {ButtonModule} from 'primeng/button';
import {InputNumberModule} from 'primeng/inputnumber';

@NgModule({
  declarations: [
    AppComponent,
    HomeComponent,
    StudentInfoComponent,
    UniversityDataComponent,
    QueryComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    HttpClientModule,
    TabMenuModule,
    TableModule,
    DropdownModule,
    FormsModule,
    ButtonModule,
    AgGridModule,
    InputNumberModule,
  ],
  providers: [HomeService],
  bootstrap: [AppComponent]
})
export class AppModule { }
