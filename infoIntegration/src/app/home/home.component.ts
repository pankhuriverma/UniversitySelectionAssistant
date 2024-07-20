import { Component, OnInit } from '@angular/core';
import { HomeService } from '../home.service';
import { MenuItem } from 'primeng/api';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.css']
})
export class HomeComponent implements OnInit {

  constructor(private homeService: HomeService) { }

  id: null;
  menu: MenuItem[];
  tabCount = 0;
  pyMsg = null;

  ngOnInit(): void {

    this.menu = [
      { label: 'Query Tab', icon: 'pi pi-fw pi-cloud', command: (event?: any) => this.data(event) },
      { label: 'University Data', icon: 'pi pi-fw pi-home', command: (event?: any) => this.data(event) }
    ];

  }

  data(event) {
    if (event.item.label === 'Student Data')
      this.tabCount = 1;
    else if (event.item.label === 'Query Tab')
      this.tabCount = 2;
    else
      this.tabCount = 3;
  }

}
